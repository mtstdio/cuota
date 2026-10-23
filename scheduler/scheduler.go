package scheduler

import (
	"context"
	"fmt"
	"log"
	"math"
	"sort"
	"sync"
	"time"

	"cuota-server/config"
	"cuota-server/cpa"
	"cuota-server/probe"
	"cuota-server/quota"
)

type DecisionView struct {
	ID               uint64    `json:"id"`
	At               time.Time `json:"at"`
	Provider         string    `json:"provider"`
	Model            string    `json:"model"`
	Stream           bool      `json:"stream"`
	Mode             string    `json:"mode"`
	Policy           string    `json:"policy"`
	Outcome          string    `json:"outcome"`
	Cause            string    `json:"cause,omitempty"`
	AuthID           string    `json:"auth_id,omitempty"`
	BoundAuthID      string    `json:"bound_auth_id,omitempty"`
	Applied          bool      `json:"applied"`
	AffinityOverrode bool      `json:"affinity_overrode_ranking"`
	Fallback         bool      `json:"fallback,omitempty"`
	Ranked           []any     `json:"ranked"`
	Skipped          []any     `json:"skipped"`
}

type AccountState string

const (
	StateActive   AccountState = "active"   // 当前主力使用中（最高优先级）
	StateReady    AccountState = "ready"    // 额度健康，在队列待命
	StateCooldown AccountState = "cooldown" // 5h 额度耗尽，等待重置倒计时
	StateDisabled AccountState = "disabled" // 人工停用
	StateError    AccountState = "error"    // 认证或请求异常
)

type ManagedAccount struct {
	Account       *cpa.Account        `json:"account"`
	Quota         *quota.AccountQuota `json:"quota,omitempty"`
	State         AccountState        `json:"state"`
	StateDesc     string              `json:"state_desc"`
	LastSeen      time.Time           `json:"last_seen"`
	LastResetAt   time.Time           `json:"last_reset_at,omitempty"`
	LockedResetAt time.Time           `json:"locked_reset_at,omitempty"`
	LastProbeAt   time.Time           `json:"last_probe_at,omitempty"`
}

type Manager struct {
	cfg            *config.Config
	cpaClient      *cpa.Client
	fetcher        *quota.Fetcher
	prober         *probe.Controller
	mu             sync.RWMutex
	accounts       map[string]*ManagedAccount
	activeAcctID   string
	refreshCh      chan struct{}
	decisions      []DecisionView
	nextDecisionID uint64
}

func NewManager(cfg *config.Config, cpaClient *cpa.Client, fetcher *quota.Fetcher, prober *probe.Controller) *Manager {
	return &Manager{
		cfg:       cfg,
		cpaClient: cpaClient,
		fetcher:   fetcher,
		prober:    prober,
		accounts:  make(map[string]*ManagedAccount),
		refreshCh: make(chan struct{}, 1),
		decisions: make([]DecisionView, 0, 200),
	}
}

func (m *Manager) recordDecision(d DecisionView) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recordDecisionLocked(d)
}

func (m *Manager) recordDecisionLocked(d DecisionView) {
	m.nextDecisionID++
	d.ID = m.nextDecisionID
	if d.At.IsZero() {
		d.At = time.Now()
	}
	if d.Provider == "" {
		d.Provider = "codex"
	}
	if d.Policy == "" && m.cfg != nil {
		d.Policy = m.cfg.Scheduler.Strategy
	}
	if d.Mode == "" {
		d.Mode = "active"
	}
	d.Applied = true

	m.decisions = append([]DecisionView{d}, m.decisions...)
	if len(m.decisions) > 200 {
		m.decisions = m.decisions[:200]
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

	// 1. 冷却到期直接激活 (Cooldown Expiration Immediate Probe)
	if managed.State == StateCooldown && !managed.LockedResetAt.IsZero() && now.After(managed.LockedResetAt) && q.FiveHour != nil && q.FiveHour.UsedPercent == 0 {
		log.Printf("[Scheduler] Account %s reached cooldown reset time with 0%% usage, triggering probe...", acct.ID)
		m.recordDecision(DecisionView{
			Provider: "codex",
			Model:    m.cfg.Probe.Model,
			Outcome:  "new_session",
			Cause:    fmt.Sprintf("账号 %s 5h 冷却周期结束到达重置点，自动触发 Probe 激活", acct.ID),
			AuthID:   acct.ID,
		})
		m.executeProbeAndRefresh(ctx, acct, managed)
		return
	}

	// 2. 连续两轮比对重置漂移（Drift Detection for Lazy Window）
	if q.FiveHour != nil && q.FiveHour.UsedPercent == 0 && !managed.LastResetAt.IsZero() {
		drift := q.FiveHour.ResetAt.Sub(managed.LastResetAt)
		if drift >= 3*time.Second {
			log.Printf("[Scheduler] Account %s detected reset drift (+%v), lazy window confirmed, triggering probe...", acct.ID, drift)
			m.recordDecision(DecisionView{
				Provider: "codex",
				Model:    m.cfg.Probe.Model,
				Outcome:  "new_session",
				Cause:    fmt.Sprintf("账号 %s 5h 倒计时未启动（重置点向后漂移 +%v），自动触发 Probe 激活", acct.ID, drift.Round(time.Second)),
				AuthID:   acct.ID,
			})
			m.executeProbeAndRefresh(ctx, acct, managed)
			return
		}
	}

	// 记录本次观测到的 ResetAt（供下轮漂移比对）
	if q.FiveHour != nil {
		m.mu.Lock()
		managed.LastResetAt = q.FiveHour.ResetAt
		m.mu.Unlock()
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
				m.mu.Lock()
				managed.LockedResetAt = q.FiveHour.ResetAt
				m.mu.Unlock()
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
		if currentState == StateCooldown {
			m.recordDecision(DecisionView{
				Provider: "codex",
				Model:    "codex",
				Outcome:  "rebind",
				Cause:    fmt.Sprintf("账号 %s 5h 额度已刷新恢复，状态解除冷却转为就绪待命", acct.ID),
				AuthID:   acct.ID,
			})
		}
	}
}

func (m *Manager) evaluateActiveAccount() {
	m.mu.Lock()
	defer m.mu.Unlock()

	var currentActive *ManagedAccount
	if m.activeAcctID != "" {
		currentActive = m.accounts[m.activeAcctID]
	}

	var oldActiveID string
	var oldRemaining float64

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
				oldActiveID = currentActive.Account.ID
				oldRemaining = remaining
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

		if oldActiveID != "" {
			m.recordDecisionLocked(DecisionView{
				Provider:    "codex",
				Model:       "codex",
				Outcome:     "failover",
				Cause:       fmt.Sprintf("主力账号 %s 5h 额度耗尽(剩余 %.1f%%)，自动熔断；推举 %s 晋升为主力账号", oldActiveID, oldRemaining, newActive.Account.ID),
				AuthID:      newActive.Account.ID,
				BoundAuthID: oldActiveID,
				Fallback:    true,
			})
		} else {
			m.recordDecisionLocked(DecisionView{
				Provider: "codex",
				Model:    "codex",
				Outcome:  "new_session",
				Cause:    fmt.Sprintf("推举可用账号 %s 晋升为主力账号", newActive.Account.ID),
				AuthID:   newActive.Account.ID,
			})
		}
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
	m.recordDecisionLocked(DecisionView{
		Provider: "codex",
		Model:    "codex",
		Outcome:  "rebind",
		Cause:    fmt.Sprintf("管理员手动指定 %s 为当前主力账号", accountID),
		AuthID:   accountID,
	})
	m.mu.Unlock()

	log.Printf("[Scheduler] Manually switched active account to %s", accountID)
	return m.cpaClient.UpdateAccountStatus(accountID, false, 100)
}

func (m *Manager) executeProbeAndRefresh(ctx context.Context, acct *cpa.Account, managed *ManagedAccount) {
	ev, probeErr := m.prober.Execute(ctx, acct)
	if probeErr == nil && ev.Success {
		m.mu.Lock()
		managed.LastProbeAt = time.Now()
		m.mu.Unlock()

		// 等待 2 秒让 OpenAI 配额后端刷新
		time.Sleep(2 * time.Second)
		freshQ, fetchErr := m.fetcher.Fetch(acct)
		if fetchErr == nil && freshQ != nil {
			m.mu.Lock()
			managed.Quota = freshQ
			if freshQ.FiveHour != nil {
				managed.LastResetAt = freshQ.FiveHour.ResetAt
				managed.LockedResetAt = time.Time{} // 解除冷却锁定
			}
			m.mu.Unlock()
		}
		if managed.State == StateCooldown {
			m.setAccountState(acct.ID, StateReady, "5h 额度已激活恢复，就绪待命")
		}
	}
}

func (m *Manager) ManualProbe(ctx context.Context, accountID string) (*probe.Event, error) {
	m.mu.RLock()
	ma, ok := m.accounts[accountID]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("account not found: %s", accountID)
	}

	m.recordDecision(DecisionView{
		Provider: "codex",
		Model:    m.cfg.Probe.Model,
		Outcome:  "new_session",
		Cause:    fmt.Sprintf("管理员手动触发账号 %s 的 Probe 激活探测", accountID),
		AuthID:   accountID,
	})

	ev, err := m.prober.Execute(ctx, ma.Account)
	if err == nil && ev.Success {
		m.mu.Lock()
		ma.LastProbeAt = time.Now()
		m.mu.Unlock()

		time.Sleep(2 * time.Second)
		freshQ, fetchErr := m.fetcher.Fetch(ma.Account)
		if fetchErr == nil && freshQ != nil {
			m.mu.Lock()
			ma.Quota = freshQ
			if freshQ.FiveHour != nil {
				ma.LastResetAt = freshQ.FiveHour.ResetAt
				ma.LockedResetAt = time.Time{}
			}
			m.mu.Unlock()
		}
		if ma.State == StateCooldown {
			m.setAccountState(accountID, StateReady, "5h 额度已激活恢复，就绪待命")
		}
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
	action := "启用"
	if disabled {
		action = "禁用"
		ma.State = StateDisabled
		ma.StateDesc = "已手动禁用"
		if m.activeAcctID == accountID {
			m.activeAcctID = ""
		}
	} else {
		ma.State = StateReady
		ma.StateDesc = "已启用待命"
	}
	m.recordDecisionLocked(DecisionView{
		Provider: "codex",
		Model:    "codex",
		Outcome:  "delegated",
		Cause:    fmt.Sprintf("管理员手动%s账号 %s", action, accountID),
		AuthID:   accountID,
	})
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

func (m *Manager) Overview() map[string]any {
	m.mu.RLock()
	defer m.mu.RUnlock()

	now := time.Now()
	accounts := m.buildAccountViewsLocked(now)

	summary := map[string]any{
		"codex": map[string]any{
			"total":           0,
			"eligible":        0,
			"limited":         0,
			"exhausted":       0,
			"blocked":         0,
			"unknown":         0,
			"active_sessions": 0,
			"expiring_soon":   0,
		},
		"claude": map[string]any{
			"total":           0,
			"eligible":        0,
			"limited":         0,
			"exhausted":       0,
			"blocked":         0,
			"unknown":         0,
			"active_sessions": 0,
			"expiring_soon":   0,
		},
	}

	codexSum := summary["codex"].(map[string]any)
	resets := make([]map[string]any, 0)
	var lastRefresh *time.Time

	for _, a := range accounts {
		codexSum["total"] = codexSum["total"].(int) + 1
		state := a["quota_state"].(string)
		routing := a["routing"].(string)
		switch {
		case state == "exhausted":
			codexSum["exhausted"] = codexSum["exhausted"].(int) + 1
		case routing == "eligible":
			codexSum["eligible"] = codexSum["eligible"].(int) + 1
		case routing == "blocked":
			codexSum["blocked"] = codexSum["blocked"].(int) + 1
		case state == "unknown" || state == "stale":
			codexSum["unknown"] = codexSum["unknown"].(int) + 1
		default:
			codexSum["limited"] = codexSum["limited"].(int) + 1
		}

		if nr, ok := a["next_reset"].(*time.Time); ok && nr != nil {
			if nr.After(now) && nr.Sub(now) <= 24*time.Hour {
				resets = append(resets, map[string]any{
					"auth_id":           a["id"],
					"account":           a["display"],
					"provider":          "codex",
					"resets_at":         nr,
					"remaining_percent": a["remaining_percent"],
				})
			}
		}

		if lp, ok := a["last_poll_at"].(*time.Time); ok && lp != nil {
			if lastRefresh == nil || lp.After(*lastRefresh) {
				lastRefresh = lp
			}
		}
	}

	nextUp := make([]map[string]any, 0)
	if m.activeAcctID != "" {
		nextUp = append(nextUp, map[string]any{
			"provider": "codex",
			"model":    "gpt-5.6-luna",
			"policy":   m.cfg.Scheduler.Strategy,
			"auth_id":  m.activeAcctID,
			"account":  m.activeAcctID,
			"reason":   "Primary active account",
			"fallback": false,
			"ranked":   []any{},
			"skipped":  []any{},
		})
	}

	return map[string]any{
		"now": now,
		"plugin": map[string]any{
			"version":                    "2.1.0",
			"started_at":                 now,
			"worker_running":             true,
			"worker_failed":              false,
			"state_writable":             true,
			"schedule_across_priorities": true,
		},
		"mode":         "active",
		"policy":       m.cfg.Scheduler.Strategy,
		"roster":       map[string]any{"ok": true, "at": now},
		"counters":     map[string]any{"picks": len(m.decisions), "routed": len(m.decisions)},
		"summary":      summary,
		"upcoming":     resets,
		"last_refresh": lastRefresh,
		"next_up":      nextUp,
		"accounts":     accounts,
		"affinity": map[string]any{
			"enabled": false,
			"ttl":     "24h",
			"scope":   "session",
		},
	}
}

func (m *Manager) AccountsView() []map[string]any {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.buildAccountViewsLocked(time.Now())
}

func (m *Manager) buildAccountViewsLocked(now time.Time) []map[string]any {
	list := make([]map[string]any, 0)
	for _, ma := range m.accounts {
		acct := ma.Account
		if acct == nil {
			continue
		}

		display := acct.Email
		if display == "" {
			display = acct.ID
		}

		windows := make([]map[string]any, 0)
		var nextReset *time.Time
		remPercent := 100.0

		if ma.Quota != nil {
			if ma.Quota.FiveHour != nil {
				used := ma.Quota.FiveHour.UsedPercent
				rem := math.Max(0, 100.0-used)
				remPercent = rem
				resetsAt := ma.Quota.FiveHour.ResetAt
				if !resetsAt.IsZero() {
					nextReset = &resetsAt
				}
				windows = append(windows, map[string]any{
					"key":               "five_hour",
					"kind":              "five_hour",
					"label":             "5-Hour Quota",
					"used_percent":      used,
					"remaining_percent": rem,
					"resets_at":         nextReset,
					"source":            "poll",
					"observed_at":       ma.Quota.FetchedAt,
					"stale":             false,
					"reset_passed":      !resetsAt.IsZero() && now.After(resetsAt),
				})
			}
			if ma.Quota.Weekly != nil {
				used := ma.Quota.Weekly.UsedPercent
				rem := math.Max(0, 100.0-used)
				var wReset *time.Time
				if !ma.Quota.Weekly.ResetAt.IsZero() {
					t := ma.Quota.Weekly.ResetAt
					wReset = &t
				}
				windows = append(windows, map[string]any{
					"key":               "weekly",
					"kind":              "weekly",
					"label":             "Weekly Quota",
					"used_percent":      used,
					"remaining_percent": rem,
					"resets_at":         wReset,
					"source":            "poll",
					"observed_at":       ma.Quota.FetchedAt,
					"stale":             false,
					"reset_passed":      wReset != nil && now.After(*wReset),
				})
			}
		}

		quotaState := "ready"
		routing := "eligible"
		switch ma.State {
		case StateCooldown:
			quotaState = "exhausted"
			routing = "limited"
		case StateDisabled:
			quotaState = "ready"
			routing = "blocked"
		case StateError:
			quotaState = "stale"
			routing = "limited"
		case StateActive:
			quotaState = "ready"
			routing = "eligible"
		case StateReady:
			quotaState = "ready"
			routing = "eligible"
		}

		plan := "Codex Subscription"
		if ma.Quota != nil && ma.Quota.PlanType != "" {
			plan = ma.Quota.PlanType
		}

		var lastPoll *time.Time
		if ma.Quota != nil && !ma.Quota.FetchedAt.IsZero() {
			t := ma.Quota.FetchedAt
			lastPoll = &t
		}

		isPreferred := (m.activeAcctID == acct.ID)

		item := map[string]any{
			"id":                acct.ID,
			"display":           display,
			"alias":             acct.Email,
			"provider":          "codex",
			"plan":              plan,
			"present":           true,
			"disabled":          acct.Disabled,
			"unavailable":       false,
			"host_priority":     acct.Priority,
			"plugin_priority":   acct.Priority,
			"polled":            ma.Quota != nil,
			"poll_error":        ma.StateDesc,
			"last_poll_at":      lastPoll,
			"last_refresh_at":   lastPoll,
			"last_observed_at":  lastPoll,
			"windows":           windows,
			"quota_state":       quotaState,
			"quota_reason":      ma.StateDesc,
			"routing":           routing,
			"routing_reason":    ma.StateDesc,
			"next_reset":        nextReset,
			"remaining_percent": remPercent,
			"expiring_soon":     nextReset != nil && nextReset.After(now) && nextReset.Sub(now) <= 24*time.Hour,
			"active_sessions":   0,
			"preferred":         isPreferred,
			"rank":              1,
			"health": map[string]any{
				"auth_failed":     ma.State == StateError,
				"auth_suspect":    false,
				"last_success_at": lastPoll,
			},
			"burn": map[string]any{},
		}
		list = append(list, item)
	}

	sort.Slice(list, func(i, j int) bool {
		prefI := list[i]["preferred"].(bool)
		prefJ := list[j]["preferred"].(bool)
		if prefI != prefJ {
			return prefI
		}
		return list[i]["remaining_percent"].(float64) > list[j]["remaining_percent"].(float64)
	})

	return list
}

func (m *Manager) Decisions(limit int) []DecisionView {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if limit <= 0 || limit > len(m.decisions) {
		limit = len(m.decisions)
	}
	res := make([]DecisionView, limit)
	copy(res, m.decisions[:limit])
	return res
}

func (m *Manager) SettingsView() map[string]any {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return map[string]any{
		"config": map[string]any{
			"mode":                  "active",
			"strategy":              m.cfg.Scheduler.Strategy,
			"poll_interval":         m.cfg.Quota.PollInterval.String(),
			"min_remaining_percent": m.cfg.Quota.MinRemainingPercent,
			"probe_enabled":         m.cfg.Probe.Enabled,
			"probe_model":           m.cfg.Probe.Model,
			"probe_endpoint":        m.cfg.Probe.Endpoint,
			"cpa_base_url":          m.cfg.CPA.BaseURL,
			"auth_dir":              m.cfg.CPA.AuthDir,
		},
		"schema":     map[string]any{},
		"generation": "1",
		"patch_path": "/v0/management/plugins/kpatdev-quota-router/config",
	}
}

func (m *Manager) Labels() map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	labels := make(map[string]string)
	for _, ma := range m.accounts {
		if ma.Account != nil {
			if ma.Account.Email != "" {
				labels[ma.Account.ID] = ma.Account.Email
			} else {
				labels[ma.Account.ID] = ma.Account.ID
			}
		}
	}
	return labels
}

func (m *Manager) Config() *config.Config {
	return m.cfg
}
