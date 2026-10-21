package scheduler

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"cuota-server/config"
	"cuota-server/cpa"
	"cuota-server/probe"
	"cuota-server/quota"
)

type AccountState string

const (
	StateActive   AccountState = "active"   // 当前主力使用中（最高优先级）
	StateReady    AccountState = "ready"    // 额度健康，在队列待命
	StateCooldown AccountState = "cooldown" // 5h 额度耗尽，等待重置倒计时
	StateDisabled AccountState = "disabled" // 人工停用
	StateError    AccountState = "error"    // 认证或请求异常
)

type ManagedAccount struct {
	Account   *cpa.Account        `json:"account"`
	Quota     *quota.AccountQuota `json:"quota,omitempty"`
	State     AccountState        `json:"state"`
	StateDesc string              `json:"state_desc"`
	LastSeen  time.Time           `json:"last_seen"`
}

type Manager struct {
	cfg          *config.Config
	cpaClient    *cpa.Client
	fetcher      *quota.Fetcher
	prober       *probe.Controller
	mu           sync.RWMutex
	accounts     map[string]*ManagedAccount
	activeAcctID string
	refreshCh    chan struct{}
}

func NewManager(cfg *config.Config, cpaClient *cpa.Client, fetcher *quota.Fetcher, prober *probe.Controller) *Manager {
	return &Manager{
		cfg:       cfg,
		cpaClient: cpaClient,
		fetcher:   fetcher,
		prober:    prober,
		accounts:  make(map[string]*ManagedAccount),
		refreshCh: make(chan struct{}, 1),
	}
}

func (m *Manager) Start(ctx context.Context) {
	log.Println("[Scheduler] Starting Quota & Account Management Engine...")
	m.reconcile(ctx)

	pollInterval := m.cfg.Quota.PollInterval
	if pollInterval < 10*time.Second {
		pollInterval = 60 * time.Second
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[Scheduler] Engine stopped.")
			return
		case <-ticker.C:
			m.reconcile(ctx)
		case <-m.refreshCh:
			m.reconcile(ctx)
		}
	}
}

func (m *Manager) TriggerRefresh() {
	select {
	case m.refreshCh <- struct{}{}:
	default:
	}
}

func (m *Manager) reconcile(ctx context.Context) {
	accounts, err := m.cpaClient.LoadAccounts()
	if err != nil {
		log.Printf("[Scheduler] Error loading accounts: %v", err)
		return
	}

	m.mu.Lock()
	for _, acct := range accounts {
		if _, exists := m.accounts[acct.ID]; !exists {
			m.accounts[acct.ID] = &ManagedAccount{
				Account:  acct,
				State:    StateReady,
				LastSeen: time.Now(),
			}
		} else {
			m.accounts[acct.ID].Account = acct
			m.accounts[acct.ID].LastSeen = time.Now()
		}
	}
	m.mu.Unlock()

	// 1. 轮询各账号最新额度与异常状态
	for _, acct := range accounts {
		if ctx.Err() != nil {
			return
		}
		m.reconcileSingleAccount(ctx, acct)
	}

	// 2. 执行 Fill-First 调度决断与主账号切换
	m.evaluateActiveAccount()
}

func (m *Manager) reconcileSingleAccount(ctx context.Context, acct *cpa.Account) {
	q, err := m.fetcher.Fetch(acct)

	m.mu.Lock()
	managed, ok := m.accounts[acct.ID]
	if !ok {
		m.mu.Unlock()
		return
	}
	managed.Quota = q
	m.mu.Unlock()

	if err != nil {
		m.setAccountState(acct.ID, StateError, fmt.Sprintf("额度查询失败: %v", err))
		return
	}

	if acct.Disabled {
		m.setAccountState(acct.ID, StateDisabled, "手动禁用")
		return
	}

	now := time.Now()

	// 检查 5h 自动激活条件（Probe ping）
	if m.prober.ShouldProbe(q) {
		log.Printf("[Scheduler] Account %s meets 5h reset probe condition, triggering activation...", acct.ID)
		_, probeErr := m.prober.Execute(ctx, acct)
		if probeErr == nil {
			// 激活成功后，重新获取一次额度以确认倒计时和配额已经启动
			time.Sleep(1 * time.Second)
			freshQ, _ := m.fetcher.Fetch(acct)
			m.mu.Lock()
			if freshQ != nil {
				managed.Quota = freshQ
				q = freshQ
			}
			m.mu.Unlock()
		}
	}

	// 判断是否耗尽进入 Cooldown
	minRemaining := m.cfg.Quota.MinRemainingPercent
	if minRemaining <= 0 {
		minRemaining = 2.0
	}

	if q.FiveHour != nil {
		remaining := 100.0 - q.FiveHour.UsedPercent
		if remaining <= minRemaining {
			desc := fmt.Sprintf("5h 额度已用尽 (剩余 %.1f%%)", remaining)
			if !q.FiveHour.ResetAt.IsZero() && q.FiveHour.ResetAt.After(now) {
				countdown := q.FiveHour.ResetAt.Sub(now).Round(time.Minute)
				desc += fmt.Sprintf("，预计 %s 后重置", countdown)
			}
			m.setAccountState(acct.ID, StateCooldown, desc)
			return
		}
	}

	// 额度健康
	m.mu.Lock()
	currentState := managed.State
	m.mu.Unlock()

	if currentState == StateCooldown || currentState == StateError || currentState == "" {
		m.setAccountState(acct.ID, StateReady, "额度健康就绪")
	}
}

func (m *Manager) evaluateActiveAccount() {
	m.mu.Lock()
	defer m.mu.Unlock()

	var currentActive *ManagedAccount
	if m.activeAcctID != "" {
		currentActive = m.accounts[m.activeAcctID]
	}

	// 检查当前主账号是否依然健康
	if currentActive != nil && currentActive.State == StateActive {
		minRemaining := m.cfg.Quota.MinRemainingPercent
		if currentActive.Quota != nil && currentActive.Quota.FiveHour != nil {
			remaining := 100.0 - currentActive.Quota.FiveHour.UsedPercent
			if remaining <= minRemaining {
				log.Printf("[Scheduler] Primary active account %s exhausted (remaining %.1f%%), demoting...", currentActive.Account.ID, remaining)
				currentActive.State = StateCooldown
				currentActive.StateDesc = "5h 额度耗尽，已切入冷却"
				_ = m.cpaClient.UpdateAccountStatus(currentActive.Account.ID, true, 0)
				currentActive = nil
				m.activeAcctID = ""
			}
		}
	} else if currentActive != nil && currentActive.State != StateActive {
		// 已不是 active 状态
		currentActive = nil
		m.activeAcctID = ""
	}

	if currentActive != nil {
		// 当前主账号健康，继续保持 Fill-First
		return
	}

	// 需要推举新的主账号
	candidates := make([]*ManagedAccount, 0)
	for _, ma := range m.accounts {
		if ma.State == StateReady && (ma.Account == nil || !ma.Account.Disabled) {
			candidates = append(candidates, ma)
		}
	}

	if len(candidates) == 0 {
		log.Println("[Scheduler] Notice: No ready accounts available in pool. Waiting for cooldown resets.")
		return
	}

	// 优先选 5h 剩余额度最充足的账号（Fill-First 主备递补）
	sort.Slice(candidates, func(i, j int) bool {
		var remI, remJ float64 = 100, 100
		if candidates[i].Quota != nil && candidates[i].Quota.FiveHour != nil {
			remI = 100 - candidates[i].Quota.FiveHour.UsedPercent
		}
		if candidates[j].Quota != nil && candidates[j].Quota.FiveHour != nil {
			remJ = 100 - candidates[j].Quota.FiveHour.UsedPercent
		}
		return remI > remJ
	})

	newActive := candidates[0]
	newActive.State = StateActive
	newActive.StateDesc = "主力分配中（最高优先级）"
	if newActive.Account != nil {
		m.activeAcctID = newActive.Account.ID
		log.Printf("[Scheduler] Promoted account %s as PRIMARY ACTIVE (priority=100)", newActive.Account.ID)
		_ = m.cpaClient.UpdateAccountStatus(newActive.Account.ID, false, 100)
	}

	// 其他 ready 账号设为备用可用 (priority=10)
	for _, c := range candidates[1:] {
		if c.Account != nil {
			_ = m.cpaClient.UpdateAccountStatus(c.Account.ID, false, 10)
		}
	}
}

func (m *Manager) setAccountState(id string, state AccountState, desc string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ma, ok := m.accounts[id]
	if !ok {
		return
	}

	if ma.State != state || ma.StateDesc != desc {
		ma.State = state
		ma.StateDesc = desc
		log.Printf("[Scheduler] Account %s state changed -> %s (%s)", id, state, desc)
	}
}

func (m *Manager) ManualSwitch(accountID string) error {
	m.mu.Lock()
	target, ok := m.accounts[accountID]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("account not found: %s", accountID)
	}

	if m.activeAcctID != "" && m.activeAcctID != accountID {
		if old, exists := m.accounts[m.activeAcctID]; exists {
			old.State = StateReady
			old.StateDesc = "备用待命"
			_ = m.cpaClient.UpdateAccountStatus(old.Account.ID, false, 10)
		}
	}

	target.State = StateActive
	target.StateDesc = "手动指定主力"
	m.activeAcctID = accountID
	m.mu.Unlock()

	log.Printf("[Scheduler] Manually switched active account to %s", accountID)
	return m.cpaClient.UpdateAccountStatus(accountID, false, 100)
}

func (m *Manager) ManualProbe(ctx context.Context, accountID string) (*probe.Event, error) {
	m.mu.RLock()
	ma, ok := m.accounts[accountID]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("account not found: %s", accountID)
	}

	ev, err := m.prober.Execute(ctx, ma.Account)
	if err == nil {
		m.TriggerRefresh()
	}
	return ev, err
}

func (m *Manager) ToggleAccount(accountID string, disabled bool) error {
	m.mu.Lock()
	ma, ok := m.accounts[accountID]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("account not found: %s", accountID)
	}
	if disabled {
		ma.State = StateDisabled
		ma.StateDesc = "已手动禁用"
		if m.activeAcctID == accountID {
			m.activeAcctID = ""
		}
	} else {
		ma.State = StateReady
		ma.StateDesc = "已启用待命"
	}
	m.mu.Unlock()

	err := m.cpaClient.UpdateAccountStatus(accountID, disabled, 0)
	m.TriggerRefresh()
	return err
}

func (m *Manager) Snapshot() (activeID string, list []*ManagedAccount, probeLogs []probe.Event) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	activeID = m.activeAcctID
	list = make([]*ManagedAccount, 0, len(m.accounts))
	for _, ma := range m.accounts {
		list = append(list, ma)
	}

	sort.Slice(list, func(i, j int) bool {
		if list[i].State == StateActive {
			return true
		}
		if list[j].State == StateActive {
			return false
		}
		return list[i].Account.ID < list[j].Account.ID
	})

	probeLogs = m.prober.History()
	return activeID, list, probeLogs
}
