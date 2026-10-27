package scheduler

import (
	"context"
	"encoding/json"
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
	"cuota-server/store"
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

type CircuitState string

const (
	CircuitClosed   CircuitState = "closed"    // 正常参与调度
	CircuitOpen     CircuitState = "open"      // 熔断中，暂停调度
	CircuitHalfOpen CircuitState = "half_open" // 半开试探自愈

	CircuitFailureThreshold = 3
	CircuitOpenDuration     = 15 * time.Minute
)

type CircuitBreaker struct {
	State         CircuitState `json:"state"`
	FailureCount  int          `json:"failure_count"`
	NextAllowedAt time.Time    `json:"next_allowed_at,omitempty"`
}

type ManagedAccount struct {
	Account       *cpa.Account        `json:"account"`
	Quota         *quota.AccountQuota `json:"quota,omitempty"`
	State         AccountState        `json:"state"`
	StateDesc     string              `json:"state_desc"`
	LastSeen      time.Time           `json:"last_seen"`
	LastResetAt   time.Time           `json:"last_reset_at,omitempty"`
	LockedResetAt time.Time           `json:"locked_reset_at,omitempty"`
	LastProbeAt   time.Time           `json:"last_probe_at,omitempty"`
	Circuit       CircuitBreaker      `json:"circuit"`
}

type Manager struct {
	cfg            *config.Config
	cpaClient      *cpa.Client
	fetcher        *quota.Fetcher
	prober         *probe.Controller
	store          *store.Store
	mu             sync.RWMutex
	accounts       map[string]*ManagedAccount
	accountAliases map[string]string
	accountPrios   map[string]int
	activeAcctIDs  map[string]string
	activeAcctID   string
	refreshCh      chan struct{}
	decisions      []DecisionView
	nextDecisionID uint64
	lastWarmupDate string
}

func NewManager(cfg *config.Config, cpaClient *cpa.Client, fetcher *quota.Fetcher, prober *probe.Controller, st ...*store.Store) *Manager {
	var storeInstance *store.Store
	if len(st) > 0 {
		storeInstance = st[0]
	}

	m := &Manager{
		cfg:            cfg,
		cpaClient:      cpaClient,
		fetcher:        fetcher,
		prober:         prober,
		store:          storeInstance,
		accounts:       make(map[string]*ManagedAccount),
		accountAliases: make(map[string]string),
		accountPrios:   make(map[string]int),
		activeAcctIDs:  make(map[string]string),
		refreshCh:      make(chan struct{}, 1),
		decisions:      make([]DecisionView, 0, 200),
	}

	if storeInstance != nil {
		if loaded, err := storeInstance.Load(); err == nil && loaded != nil {
			if loaded.Strategy != "" {
				m.cfg.Scheduler.Strategy = loaded.Strategy
			}
			if loaded.AccountAliases != nil {
				m.accountAliases = loaded.AccountAliases
			}
			if loaded.AccountPriority != nil {
				m.accountPrios = loaded.AccountPriority
			}
			if loaded.LastWarmupDate != "" {
				m.lastWarmupDate = loaded.LastWarmupDate
			}
			if loaded.Timezone != "" {
				m.cfg.Probe.Timezone = loaded.Timezone
			}
			if loaded.QuietHoursEnabled != nil {
				m.cfg.Probe.QuietHoursEnabled = *loaded.QuietHoursEnabled
			}
			if loaded.QuietHoursStart != "" {
				m.cfg.Probe.QuietHoursStart = loaded.QuietHoursStart
			}
			if loaded.QuietHoursEnd != "" {
				m.cfg.Probe.QuietHoursEnd = loaded.QuietHoursEnd
			}
			if loaded.MorningWarmupEnabled != nil {
				m.cfg.Probe.MorningWarmupEnabled = *loaded.MorningWarmupEnabled
			}
			if loaded.MorningWarmupTime != "" {
				m.cfg.Probe.MorningWarmupTime = loaded.MorningWarmupTime
			}
			if m.prober != nil {
				m.prober.UpdateConfig(m.cfg.Probe)
			}
			if len(loaded.Decisions) > 0 {
				for _, raw := range loaded.Decisions {
					if b, err := json.Marshal(raw); err == nil {
						var d DecisionView
						if err := json.Unmarshal(b, &d); err == nil {
							m.decisions = append(m.decisions, d)
							if d.ID >= m.nextDecisionID {
								m.nextDecisionID = d.ID
							}
						}
					}
				}
			}
		}
	}

	return m
}

func (m *Manager) saveStateLocked() {
	if m.store == nil {
		return
	}
	decList := make([]any, 0, len(m.decisions))
	for _, d := range m.decisions {
		decList = append(decList, d)
	}
	aliasesCopy := make(map[string]string, len(m.accountAliases))
	for k, v := range m.accountAliases {
		aliasesCopy[k] = v
	}
	priosCopy := make(map[string]int, len(m.accountPrios))
	for k, v := range m.accountPrios {
		priosCopy[k] = v
	}

	quietHoursEnabled := m.cfg.Probe.QuietHoursEnabled
	morningWarmupEnabled := m.cfg.Probe.MorningWarmupEnabled

	st := &store.State{
		Strategy:             m.cfg.Scheduler.Strategy,
		AccountAliases:       aliasesCopy,
		AccountPriority:      priosCopy,
		Decisions:            decList,
		LastWarmupDate:       m.lastWarmupDate,
		Timezone:             m.cfg.Probe.Timezone,
		QuietHoursEnabled:    &quietHoursEnabled,
		QuietHoursStart:      m.cfg.Probe.QuietHoursStart,
		QuietHoursEnd:        m.cfg.Probe.QuietHoursEnd,
		MorningWarmupEnabled: &morningWarmupEnabled,
		MorningWarmupTime:    m.cfg.Probe.MorningWarmupTime,
		UpdatedAt:            time.Now(),
	}
	if err := m.store.Save(st); err != nil {
		log.Printf("[Scheduler] Error saving state: %v", err)
	}
}

func (m *Manager) UpdateProbeConfig(timezone string, quietHoursEnabled *bool, quietStart, quietEnd string, warmupEnabled *bool, warmupTime string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if timezone != "" {
		m.cfg.Probe.Timezone = timezone
	}
	if quietHoursEnabled != nil {
		m.cfg.Probe.QuietHoursEnabled = *quietHoursEnabled
	}
	if quietStart != "" {
		m.cfg.Probe.QuietHoursStart = quietStart
	}
	if quietEnd != "" {
		m.cfg.Probe.QuietHoursEnd = quietEnd
	}
	if warmupEnabled != nil {
		m.cfg.Probe.MorningWarmupEnabled = *warmupEnabled
	}
	if warmupTime != "" {
		m.cfg.Probe.MorningWarmupTime = warmupTime
	}

	if m.prober != nil {
		m.prober.UpdateConfig(m.cfg.Probe)
	}
	m.saveStateLocked()
}

func (m *Manager) SetStrategy(strategy string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cfg.Scheduler.Strategy = strategy
	m.saveStateLocked()
	m.TriggerRefresh()
	return nil
}

func (m *Manager) SetAccountAliases(aliases map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range aliases {
		m.accountAliases[k] = v
	}
	m.saveStateLocked()
}

func (m *Manager) SetAccountPriorities(prios map[string]int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range prios {
		m.accountPrios[k] = v
	}
	m.saveStateLocked()
	m.TriggerRefresh()
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
	m.saveStateLocked()
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
		ma, exists := m.accounts[acct.ID]
		if !exists {
			m.accounts[acct.ID] = &ManagedAccount{
				Account:  acct,
				State:    StateReady,
				LastSeen: time.Now(),
				Circuit:  CircuitBreaker{State: CircuitClosed},
			}
		} else {
			// 检测凭据文件是否被重新写入或 token 发生变更（如操作员重新登录）
			tokenChanged := (ma.Account != nil && acct.AccessToken != "" && acct.AccessToken != ma.Account.AccessToken)
			mtimeChanged := (!acct.ModTime.IsZero() && ma.Account != nil && !ma.Account.ModTime.IsZero() && acct.ModTime.After(ma.Account.ModTime))

			if tokenChanged || mtimeChanged {
				if ma.State == StateError || ma.Circuit.State != CircuitClosed {
					log.Printf("[Scheduler] Detected credential update/re-login for account %s, resetting error & circuit state...", acct.ID)
					ma.State = StateReady
					ma.StateDesc = "凭据已更新，重置错误状态"
					ma.Circuit.State = CircuitClosed
					ma.Circuit.FailureCount = 0
					ma.Circuit.NextAllowedAt = time.Time{}
				}
			}

			ma.Account = acct
			ma.LastSeen = time.Now()
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

	// 3. 检查晨间批量预热（东八区 07:30）
	m.checkMorningWarmup(ctx, time.Now())
}

func (m *Manager) reconcileSingleAccount(ctx context.Context, acct *cpa.Account) {
	now := time.Now()

	m.mu.Lock()
	managed, ok := m.accounts[acct.ID]
	if !ok {
		m.mu.Unlock()
		return
	}

	// 熔断状态保护判定
	if managed.Circuit.State == CircuitOpen {
		if now.Before(managed.Circuit.NextAllowedAt) {
			m.mu.Unlock()
			return // 处于熔断冷却期内，跳过请求，防止持续冲击故障上游
		}
		// 熔断冷却期结束，转入半开试探
		managed.Circuit.State = CircuitHalfOpen
		log.Printf("[Scheduler] Account %s circuit entering HALF-OPEN, attempting probe/refresh...", acct.ID)
	}
	m.mu.Unlock()

	q, err := m.fetcher.Fetch(acct)

	m.mu.Lock()
	managed.Quota = q
	if err != nil {
		managed.Circuit.FailureCount++
		if managed.Circuit.FailureCount >= CircuitFailureThreshold {
			managed.Circuit.State = CircuitOpen
			managed.Circuit.NextAllowedAt = now.Add(CircuitOpenDuration)
			desc := fmt.Sprintf("服务异常已熔断 (连续失败 %d 次)，预计 %s 后试探自愈: %v",
				managed.Circuit.FailureCount, CircuitOpenDuration.Round(time.Minute), err)
			managed.State = StateError
			managed.StateDesc = desc
			m.mu.Unlock()
			log.Printf("[Scheduler] Account %s tripped CIRCUIT OPEN: %s", acct.ID, desc)
			return
		}

		desc := fmt.Sprintf("额度查询失败 (%d/%d): %v",
			managed.Circuit.FailureCount, CircuitFailureThreshold, err)
		managed.State = StateError
		managed.StateDesc = desc
		m.mu.Unlock()
		return
	}

	// Fetch 成功：如果之前有失败或处于半开状态，恢复为正常 Closed
	if managed.Circuit.State == CircuitHalfOpen || managed.Circuit.FailureCount > 0 {
		log.Printf("[Scheduler] Account %s health restored, resetting circuit to CLOSED", acct.ID)
		managed.Circuit.State = CircuitClosed
		managed.Circuit.FailureCount = 0
		managed.Circuit.NextAllowedAt = time.Time{}
	}
	m.mu.Unlock()

	if acct.Disabled {
		m.setAccountState(acct.ID, StateDisabled, "手动禁用")
		return
	}

	now = time.Now()

	// 探测仅针对 Codex 账号（Claude 无需也无 ChatGPT responses 探活接口）
	if acct.Provider != "claude" {
		inQuiet := m.prober != nil && m.prober.IsInQuietHours(now)

		if !inQuiet {
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
		}

		// 记录本次观测到的 ResetAt（供下轮漂移比对）
		if q.FiveHour != nil {
			m.mu.Lock()
			managed.LastResetAt = q.FiveHour.ResetAt
			m.mu.Unlock()
		}
	}

	// 判断是否耗尽进入 Cooldown
	minRemaining := m.cfg.Quota.MinRemainingPercent
	if minRemaining <= 0 {
		minRemaining = 2.0
	}

	// Codex 5h 窗口或 Claude 5h/7d 窗口检查
	var checkWindow *quota.Window
	windowName := "5h"
	if q.FiveHour != nil {
		checkWindow = q.FiveHour
	} else if q.Weekly != nil {
		checkWindow = q.Weekly
		windowName = "周"
	}

	if checkWindow != nil {
		remaining := 100.0 - checkWindow.UsedPercent
		if remaining <= minRemaining {
			desc := fmt.Sprintf("%s 额度已用尽 (剩余 %.1f%%)", windowName, remaining)
			if !checkWindow.ResetAt.IsZero() && checkWindow.ResetAt.After(now) {
				countdown := checkWindow.ResetAt.Sub(now).Round(time.Minute)
				desc += fmt.Sprintf("，预计 %s 后重置", countdown)
				m.mu.Lock()
				managed.LockedResetAt = checkWindow.ResetAt
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
			prov := acct.Provider
			if prov == "" {
				prov = "codex"
			}
			m.recordDecision(DecisionView{
				Provider: prov,
				Model:    prov,
				Outcome:  "rebind",
				Cause:    fmt.Sprintf("账号 %s 额度已刷新恢复，状态解除冷却转为就绪待命", acct.ID),
				AuthID:   acct.ID,
			})
		}
	}
}

func (m *Manager) evaluateActiveAccount() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	minRemaining := m.cfg.Quota.MinRemainingPercent
	if minRemaining <= 0 {
		minRemaining = 2.0
	}

	// 按 Provider 分组评选
	byProvider := make(map[string][]*ManagedAccount)
	for _, ma := range m.accounts {
		if ma.Account == nil {
			continue
		}
		prov := ma.Account.Provider
		if prov == "" {
			prov = "codex"
		}
		byProvider[prov] = append(byProvider[prov], ma)
	}

	for prov, accts := range byProvider {
		currentActiveID := m.activeAcctIDs[prov]
		if currentActiveID == "" && prov == "codex" {
			currentActiveID = m.activeAcctID
		}

		candidates := make([]Candidate, 0, len(accts))
		for _, ma := range accts {
			var remaining, used float64 = 100.0, 0.0
			var resetAt time.Time
			isUsable := false

			if ma.Quota != nil {
				isUsable = ma.Quota.IsUsable
				if ma.Quota.FiveHour != nil {
					used = ma.Quota.FiveHour.UsedPercent
					remaining = math.Max(0, 100.0-used)
					resetAt = ma.Quota.FiveHour.ResetAt
				} else if ma.Quota.Weekly != nil {
					used = ma.Quota.Weekly.UsedPercent
					remaining = math.Max(0, 100.0-used)
					resetAt = ma.Quota.Weekly.ResetAt
				}
			}

			prio := ma.Account.Priority
			if p, ok := m.accountPrios[ma.Account.ID]; ok {
				prio = p
			}

			candidates = append(candidates, Candidate{
				AccountID:      ma.Account.ID,
				Provider:       prov,
				PluginPriority: prio,
				HostPriority:   ma.Account.Priority,
				Remaining:      remaining,
				FiveHourUsed:   used,
				ResetsAt:       resetAt,
				IsUsable:       isUsable,
				Disabled:       ma.Account.Disabled || ma.State == StateDisabled,
				CircuitOpen:    ma.Circuit.State == CircuitOpen,
			})
		}

		strategy := m.cfg.Scheduler.Strategy
		if strategy == "" {
			strategy = StrategyFillFirst
		}

		ranked, skipped := RankCandidates(candidates, strategy, now, currentActiveID, minRemaining)

		rankedAny := make([]any, len(ranked))
		for i, r := range ranked {
			rankedAny[i] = r
		}
		skippedAny := make([]any, len(skipped))
		for i, s := range skipped {
			skippedAny[i] = s
		}

		if len(ranked) > 0 {
			winnerID := ranked[0].AccountID
			winnerAcct := m.accounts[winnerID]

			if winnerID != currentActiveID {
				log.Printf("[Scheduler] [%s] Promoting %s as PRIMARY ACTIVE (strategy: %s, score: %.2f)", prov, winnerID, strategy, ranked[0].Score)
				if currentActiveID != "" {
					if oldMa, ok := m.accounts[currentActiveID]; ok {
						if oldMa.State == StateActive {
							var oldRem float64 = 100.0
							if oldMa.Quota != nil {
								if oldMa.Quota.FiveHour != nil {
									oldRem = 100.0 - oldMa.Quota.FiveHour.UsedPercent
								} else if oldMa.Quota.Weekly != nil {
									oldRem = 100.0 - oldMa.Quota.Weekly.UsedPercent
								}
							}
							if oldRem <= minRemaining {
								oldMa.State = StateCooldown
								oldMa.StateDesc = fmt.Sprintf("额度已耗尽 (剩余 %.1f%%)，切入冷却", oldRem)
								_ = m.cpaClient.UpdateAccountStatus(oldMa.Account.ID, true, 0)
							} else {
								oldMa.State = StateReady
								oldMa.StateDesc = "备用待命"
								_ = m.cpaClient.UpdateAccountStatus(oldMa.Account.ID, false, 10)
							}
						}
					}
				}

				winnerAcct.State = StateActive
				winnerAcct.StateDesc = "主力分配中（最高优先级）"
				_ = m.cpaClient.UpdateAccountStatus(winnerAcct.Account.ID, false, 100)

				m.activeAcctIDs[prov] = winnerID
				if prov == "codex" || m.activeAcctID == "" {
					m.activeAcctID = winnerID
				}

				m.recordDecisionLocked(DecisionView{
					Provider:         prov,
					Model:            prov,
					Policy:           strategy,
					Outcome:          "new_session",
					Cause:            fmt.Sprintf("[%s] 调度策略 %s 推举 %s 晋升主力账号 (得分: %.2f)", prov, strategy, winnerID, ranked[0].Score),
					AuthID:           winnerID,
					BoundAuthID:      currentActiveID,
					Ranked:           rankedAny,
					Skipped:          skippedAny,
					AffinityOverrode: false,
					Fallback:         currentActiveID != "",
				})
			} else {
				winnerAcct.State = StateActive
				winnerAcct.StateDesc = "主力分配中（最高优先级）"
			}

			// 其他就绪候选账号更新为备用状态 (priority=10)
			for _, r := range ranked[1:] {
				if standby, ok := m.accounts[r.AccountID]; ok {
					if standby.State == StateActive {
						standby.State = StateReady
						standby.StateDesc = "备用待命"
					}
					_ = m.cpaClient.UpdateAccountStatus(standby.Account.ID, false, 10)
				}
			}
		} else {
			if currentActiveID != "" {
				log.Printf("[Scheduler] [%s] Notice: No eligible accounts available in pool. Demoting %s", prov, currentActiveID)
				m.activeAcctIDs[prov] = ""
				if m.activeAcctID == currentActiveID {
					m.activeAcctID = ""
				}
			}
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

	prov := "codex"
	if target.Account != nil && target.Account.Provider != "" {
		prov = target.Account.Provider
	}

	oldActiveID := m.activeAcctIDs[prov]
	if oldActiveID == "" && prov == "codex" {
		oldActiveID = m.activeAcctID
	}

	if oldActiveID != "" && oldActiveID != accountID {
		if old, exists := m.accounts[oldActiveID]; exists {
			old.State = StateReady
			old.StateDesc = "备用待命"
			_ = m.cpaClient.UpdateAccountStatus(old.Account.ID, false, 10)
		}
	}

	target.State = StateActive
	target.StateDesc = "手动指定主力"
	m.activeAcctIDs[prov] = accountID
	if prov == "codex" || m.activeAcctID == "" {
		m.activeAcctID = accountID
	}

	m.recordDecisionLocked(DecisionView{
		Provider: prov,
		Model:    prov,
		Outcome:  "rebind",
		Cause:    fmt.Sprintf("管理员手动指定 %s 为当前 [%s] 主力账号", accountID, prov),
		AuthID:   accountID,
	})
	m.mu.Unlock()

	log.Printf("[Scheduler] Manually switched [%s] active account to %s", prov, accountID)
	return m.cpaClient.UpdateAccountStatus(accountID, false, 100)
}

func (m *Manager) executeProbeAndRefresh(ctx context.Context, acct *cpa.Account, managed *ManagedAccount) {
	ev, probeErr := m.prober.Execute(ctx, acct)
	if probeErr == nil && ev.Success {
		m.mu.Lock()
		managed.LastProbeAt = time.Now()
		m.mu.Unlock()

		time.Sleep(2 * time.Second)
		freshQ, fetchErr := m.fetcher.Fetch(acct)
		if fetchErr == nil && freshQ != nil {
			m.mu.Lock()
			managed.Quota = freshQ
			if freshQ.FiveHour != nil {
				managed.LastResetAt = freshQ.FiveHour.ResetAt
				managed.LockedResetAt = time.Time{}
			}
			m.mu.Unlock()
		}
		if managed.State == StateCooldown {
			m.setAccountState(acct.ID, StateReady, "5h 额度已激活恢复，就绪待命")
		}
	}
}

func (m *Manager) checkMorningWarmup(ctx context.Context, now time.Time) {
	m.mu.Lock()
	cfg := m.cfg.Probe
	loc := probe.ParseLocation(cfg.Timezone)
	localNow := now.In(loc)
	todayStr := localNow.Format("2006-01-02")

	if !cfg.Enabled || !cfg.MorningWarmupEnabled {
		m.mu.Unlock()
		return
	}

	if m.lastWarmupDate == todayStr {
		m.mu.Unlock()
		return
	}

	warmupMin, err := probe.ParseTimeMinutes(cfg.MorningWarmupTime)
	if err != nil {
		m.mu.Unlock()
		return
	}

	curMin := localNow.Hour()*60 + localNow.Minute()
	if curMin < warmupMin || curMin >= warmupMin+60 {
		m.mu.Unlock()
		return
	}

	m.lastWarmupDate = todayStr
	m.saveStateLocked()
	m.mu.Unlock()

	log.Printf("[Scheduler] Morning warmup triggered for %s (local time %s, timezone %s)", todayStr, localNow.Format("15:04:05"), cfg.Timezone)
	m.executeMorningWarmup(ctx, cfg.MorningWarmupTime)
}

func (m *Manager) executeMorningWarmup(ctx context.Context, warmupTime string) {
	if m.prober == nil {
		return
	}

	m.mu.RLock()
	var toWarmup []*cpa.Account
	for _, ma := range m.accounts {
		if ma.Account == nil || ma.Account.Disabled || ma.Account.Provider == "claude" {
			continue
		}
		if ma.Circuit.State == CircuitOpen {
			continue
		}
		toWarmup = append(toWarmup, ma.Account)
	}
	m.mu.RUnlock()

	if len(toWarmup) == 0 {
		log.Printf("[Scheduler] Morning warmup: no eligible codex accounts to probe")
		return
	}

	log.Printf("[Scheduler] Morning warmup: activating %d accounts...", len(toWarmup))
	m.recordDecision(DecisionView{
		Provider: "codex",
		Model:    m.cfg.Probe.Model,
		Outcome:  "new_session",
		Cause:    fmt.Sprintf("晨间定时预热（%s），批量激活锁定 %d 个可用账号 5h 窗口 (07:30-12:30)", warmupTime, len(toWarmup)),
	})

	for _, acct := range toWarmup {
		if ctx.Err() != nil {
			return
		}
		m.mu.RLock()
		managed, ok := m.accounts[acct.ID]
		m.mu.RUnlock()
		if !ok {
			continue
		}

		ev, probeErr := m.prober.ExecuteForced(ctx, acct)
		if probeErr == nil && ev != nil && ev.Success {
			m.mu.Lock()
			managed.LastProbeAt = time.Now()
			m.mu.Unlock()

			time.Sleep(1 * time.Second)
			freshQ, fetchErr := m.fetcher.Fetch(acct)
			if fetchErr == nil && freshQ != nil {
				m.mu.Lock()
				managed.Quota = freshQ
				if freshQ.FiveHour != nil {
					managed.LastResetAt = freshQ.FiveHour.ResetAt
					managed.LockedResetAt = time.Time{}
				}
				m.mu.Unlock()
			}
			if managed.State == StateCooldown {
				m.setAccountState(acct.ID, StateReady, "晨间预热激活就绪")
			}
		} else {
			log.Printf("[Scheduler] Morning warmup probe for %s failed: %v", acct.ID, probeErr)
		}
	}
	log.Printf("[Scheduler] Morning warmup finished for %d accounts.", len(toWarmup))
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

	ev, err := m.prober.ExecuteForced(ctx, ma.Account)
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
	claudeSum := summary["claude"].(map[string]any)
	resets := make([]map[string]any, 0)
	var lastRefresh *time.Time

	for _, a := range accounts {
		prov, _ := a["provider"].(string)
		sum := codexSum
		if prov == "claude" {
			sum = claudeSum
		}
		sum["total"] = sum["total"].(int) + 1
		state := a["quota_state"].(string)
		routing := a["routing"].(string)
		switch {
		case state == "exhausted":
			sum["exhausted"] = sum["exhausted"].(int) + 1
		case routing == "eligible":
			sum["eligible"] = sum["eligible"].(int) + 1
		case routing == "blocked":
			sum["blocked"] = sum["blocked"].(int) + 1
		case state == "unknown" || state == "stale":
			sum["unknown"] = sum["unknown"].(int) + 1
		default:
			sum["limited"] = sum["limited"].(int) + 1
		}

		if nr, ok := a["next_reset"].(*time.Time); ok && nr != nil {
			if nr.After(now) && nr.Sub(now) <= 24*time.Hour {
				resets = append(resets, map[string]any{
					"auth_id":           a["id"],
					"account":           a["display"],
					"provider":          prov,
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
	for _, p := range []string{"codex", "claude"} {
		actID := m.activeAcctIDs[p]
		if actID == "" && p == "codex" {
			actID = m.activeAcctID
		}
		if actID != "" {
			disp := actID
			if alias, ok := m.accountAliases[actID]; ok && alias != "" {
				disp = alias
			}
			model := m.cfg.Probe.Model
			if p == "claude" {
				model = "claude-3-7-sonnet"
			}
			nextUp = append(nextUp, map[string]any{
				"provider": p,
				"model":    model,
				"policy":   m.cfg.Scheduler.Strategy,
				"auth_id":  actID,
				"account":  disp,
				"reason":   "Primary active account",
				"fallback": false,
				"ranked":   []any{},
				"skipped":  []any{},
			})
		}
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

		prov := acct.Provider
		if prov == "" {
			prov = "codex"
		}

		display := acct.Email
		if display == "" {
			display = acct.ID
		}
		alias := acct.Email
		if a, ok := m.accountAliases[acct.ID]; ok && a != "" {
			alias = a
			display = a
		}

		prio := acct.Priority
		if p, ok := m.accountPrios[acct.ID]; ok {
			prio = p
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

		if ma.Circuit.State == CircuitOpen {
			routing = "blocked"
		}

		plan := "Subscription"
		if prov == "codex" {
			plan = "Codex Subscription"
		} else if prov == "claude" {
			plan = "Claude Pro / Team"
		}
		if ma.Quota != nil && ma.Quota.PlanType != "" {
			plan = ma.Quota.PlanType
		}

		var lastPoll *time.Time
		if ma.Quota != nil && !ma.Quota.FetchedAt.IsZero() {
			t := ma.Quota.FetchedAt
			lastPoll = &t
		}

		isPreferred := (m.activeAcctIDs[prov] == acct.ID || m.activeAcctID == acct.ID)

		item := map[string]any{
			"id":                acct.ID,
			"display":           display,
			"alias":             alias,
			"provider":          prov,
			"plan":              plan,
			"present":           true,
			"disabled":          acct.Disabled,
			"unavailable":       false,
			"host_priority":     acct.Priority,
			"plugin_priority":   prio,
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
				"circuit_state":   ma.Circuit.State,
				"failure_count":   ma.Circuit.FailureCount,
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

	schema := []map[string]any{
		{
			"name":        "strategy",
			"type":        "enum",
			"enum":        []string{"fill_first", "reset_first", "quota_pressure", "balanced", "manual_priority"},
			"default":     "fill_first",
			"group":       "Routing",
			"description": "How accounts are prioritized (fill_first, reset_first, quota_pressure, balanced, manual_priority)",
			"effect":      "Affects primary active account selection.",
		},
		{
			"name":        "mode",
			"type":        "enum",
			"enum":        []string{"active", "shadow"},
			"default":     "active",
			"group":       "Routing",
			"description": "Active routes and promotes accounts. Shadow records proposed decisions.",
			"effect":      "Immediate.",
		},
		{
			"name":        "poll_interval",
			"type":        "duration",
			"min":         "10s",
			"max":         "1h",
			"default":     "1m",
			"group":       "Quota",
			"description": "Interval between quota polling cycles.",
			"effect":      "Applied on next cycle.",
		},
		{
			"name":        "min_remaining_percent",
			"type":        "number",
			"min":         "0",
			"max":         "50",
			"default":     2,
			"group":       "Quota",
			"description": "Minimum 5h quota remaining percent before switching account.",
			"effect":      "Applied on next quota check.",
		},
		{
			"name":        "probe_enabled",
			"type":        "boolean",
			"default":     true,
			"group":       "Diagnostics",
			"description": "Automatically send minimal ping probe to lock 5h reset countdown.",
			"effect":      "Controls background probe trigger.",
		},
		{
			"name":        "probe_model",
			"type":        "string",
			"default":     "gpt-5.6-luna",
			"group":       "Diagnostics",
			"description": "Model used for 5h activation probes.",
			"effect":      "Applied on next probe.",
		},
		{
			"name":        "quiet_hours_enabled",
			"type":        "boolean",
			"default":     true,
			"group":       "Diagnostics",
			"description": "Enable quiet hours to pause automated probes (preserves 100% quota via lazy drift).",
			"effect":      "Pauses background probe activation during the quiet window.",
		},
		{
			"name":        "quiet_hours_start",
			"type":        "string",
			"default":     "01:00",
			"group":       "Diagnostics",
			"description": "Quiet hours start time (HH:MM).",
			"effect":      "Probing paused starting at this time.",
		},
		{
			"name":        "quiet_hours_end",
			"type":        "string",
			"default":     "07:30",
			"group":       "Diagnostics",
			"description": "Quiet hours end time (HH:MM).",
			"effect":      "Probing resumed at this time.",
		},
		{
			"name":        "morning_warmup_enabled",
			"type":        "boolean",
			"default":     true,
			"group":       "Diagnostics",
			"description": "Enable daily morning warmup to lock 5h reset countdown for all healthy accounts.",
			"effect":      "Probes all eligible accounts at warmup time to lock 07:30-12:30 window.",
		},
		{
			"name":        "morning_warmup_time",
			"type":        "string",
			"default":     "07:30",
			"group":       "Diagnostics",
			"description": "Morning warmup trigger time (HH:MM).",
			"effect":      "Triggers batch probe at this time daily.",
		},
		{
			"name":        "timezone",
			"type":        "string",
			"default":     "Asia/Shanghai",
			"group":       "Diagnostics",
			"description": "Local timezone used for quiet hours and morning warmup schedule.",
			"effect":      "Timezone reference for schedules.",
		},
		{
			"name":        "auth_dir",
			"type":        "string",
			"default":     m.cfg.CPA.AuthDir,
			"group":       "Storage",
			"description": "Directory where CLIProxyAPI auth json files are located.",
			"effect":      "Account directory scanning.",
		},
	}

	return map[string]any{
		"config": map[string]any{
			"mode":                  "active",
			"strategy":              m.cfg.Scheduler.Strategy,
			"poll_interval":         m.cfg.Quota.PollInterval.String(),
			"min_remaining_percent": m.cfg.Quota.MinRemainingPercent,
			"probe_enabled":         m.cfg.Probe.Enabled,
			"probe_model":           m.cfg.Probe.Model,
			"probe_endpoint":        m.cfg.Probe.Endpoint,
			"quiet_hours_enabled":    m.cfg.Probe.QuietHoursEnabled,
			"quiet_hours_start":      m.cfg.Probe.QuietHoursStart,
			"quiet_hours_end":        m.cfg.Probe.QuietHoursEnd,
			"morning_warmup_enabled": m.cfg.Probe.MorningWarmupEnabled,
			"morning_warmup_time":    m.cfg.Probe.MorningWarmupTime,
			"timezone":               m.cfg.Probe.Timezone,
			"cpa_base_url":          m.cfg.CPA.BaseURL,
			"auth_dir":              m.cfg.CPA.AuthDir,
		},
		"schema":     schema,
		"generation": "1",
		"patch_path": "/v0/management/plugins/kpatdev-quota-router/config",
	}
}

func (m *Manager) Labels() map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	labels := make(map[string]string)
	for id, alias := range m.accountAliases {
		if alias != "" {
			labels[id] = alias
		}
	}
	for _, ma := range m.accounts {
		if ma.Account != nil {
			if _, ok := labels[ma.Account.ID]; !ok {
				if ma.Account.Email != "" {
					labels[ma.Account.ID] = ma.Account.Email
				} else {
					labels[ma.Account.ID] = ma.Account.ID
				}
			}
		}
	}
	return labels
}

func (m *Manager) Simulate(provider, model string, strategies []string) []map[string]any {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if provider == "" {
		provider = "codex"
	}
	if len(strategies) == 0 {
		strategies = []string{StrategyFillFirst, StrategyResetFirst, StrategyQuotaPressure, StrategyBalanced, StrategyManualPriority}
	}
	if model == "" {
		if provider == "claude" {
			model = "claude-3-7-sonnet"
		} else {
			model = m.cfg.Probe.Model
		}
	}

	now := time.Now()
	minRemaining := m.cfg.Quota.MinRemainingPercent
	if minRemaining <= 0 {
		minRemaining = 2.0
	}

	currentActiveID := m.activeAcctIDs[provider]
	if currentActiveID == "" && provider == "codex" {
		currentActiveID = m.activeAcctID
	}

	var candidates []Candidate
	for _, ma := range m.accounts {
		if ma.Account == nil {
			continue
		}
		p := ma.Account.Provider
		if p == "" {
			p = "codex"
		}
		if p != provider {
			continue
		}

		var remaining, used float64 = 100.0, 0.0
		var resetAt time.Time
		isUsable := false

		if ma.Quota != nil {
			isUsable = ma.Quota.IsUsable
			if ma.Quota.FiveHour != nil {
				used = ma.Quota.FiveHour.UsedPercent
				remaining = math.Max(0, 100.0-used)
				resetAt = ma.Quota.FiveHour.ResetAt
			} else if ma.Quota.Weekly != nil {
				used = ma.Quota.Weekly.UsedPercent
				remaining = math.Max(0, 100.0-used)
				resetAt = ma.Quota.Weekly.ResetAt
			}
		}

		prio := ma.Account.Priority
		if pr, ok := m.accountPrios[ma.Account.ID]; ok {
			prio = pr
		}

		candidates = append(candidates, Candidate{
			AccountID:      ma.Account.ID,
			Provider:       provider,
			PluginPriority: prio,
			HostPriority:   ma.Account.Priority,
			Remaining:      remaining,
			FiveHourUsed:   used,
			ResetsAt:       resetAt,
			IsUsable:       isUsable,
			Disabled:       ma.Account.Disabled || ma.State == StateDisabled,
			CircuitOpen:    ma.Circuit.State == CircuitOpen,
		})
	}

	results := make([]map[string]any, 0, len(strategies))
	for _, st := range strategies {
		ranked, skipped := RankCandidates(candidates, st, now, currentActiveID, minRemaining)
		var winnerID, disp, reason string
		if len(ranked) > 0 {
			winnerID = ranked[0].AccountID
			disp = winnerID
			if a, ok := m.accountAliases[winnerID]; ok && a != "" {
				disp = a
			}
			reason = ranked[0].Reason
		}
		results = append(results, map[string]any{
			"provider": provider,
			"model":    model,
			"policy":   st,
			"auth_id":  winnerID,
			"account":  disp,
			"reason":   reason,
			"ranked":   ranked,
			"skipped":  skipped,
		})
	}
	return results
}

func (m *Manager) Config() *config.Config {
	return m.cfg
}
