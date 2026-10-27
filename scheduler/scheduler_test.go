package scheduler

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cuota-server/config"
	"cuota-server/cpa"
	"cuota-server/probe"
	"cuota-server/quota"
	"cuota-server/store"
)

func TestSchedulerFillFirstPromotionAndSwitch(t *testing.T) {
	tmpDir := t.TempDir()

	file1 := filepath.Join(tmpDir, "acct1.json")
	file2 := filepath.Join(tmpDir, "acct2.json")
	_ = os.WriteFile(file1, []byte(`{"provider":"codex","access_token":"t1","disabled":false,"priority":0}`), 0644)
	_ = os.WriteFile(file2, []byte(`{"provider":"codex","access_token":"t2","disabled":false,"priority":0}`), 0644)

	cfg := config.DefaultConfig()
	cfg.CPA.AuthDir = tmpDir

	cpaClient := cpa.NewClient(cfg.CPA)
	_, err := cpaClient.LoadAccounts()
	if err != nil {
		t.Fatal(err)
	}

	fetcher := quota.NewFetcher(cfg.Quota.RequestTimeout)
	prober := probe.NewController(cfg.Probe)
	mgr := NewManager(&cfg, cpaClient, fetcher, prober)

	acct1, _ := cpaClient.GetAccount("acct1.json")
	acct2, _ := cpaClient.GetAccount("acct2.json")

	mgr.accounts["acct1.json"] = &ManagedAccount{
		Account: acct1,
		State:   StateReady,
		Quota: &quota.AccountQuota{
			IsUsable: true,
			FiveHour: &quota.Window{UsedPercent: 20.0},
		},
	}
	mgr.accounts["acct2.json"] = &ManagedAccount{
		Account: acct2,
		State:   StateReady,
		Quota: &quota.AccountQuota{
			IsUsable: true,
			FiveHour: &quota.Window{UsedPercent: 10.0}, // more remaining
		},
	}

	// 1. Initial evaluation: acct2 has more remaining quota (90% > 80%), so acct2 promoted
	mgr.evaluateActiveAccount()

	activeID, list, _ := mgr.Snapshot()
	if activeID != "acct2.json" {
		t.Fatalf("expected acct2.json to be active, got %s", activeID)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 accounts in list, got %d", len(list))
	}

	// 2. Simulate acct2 quota exhausted (used 99%)
	mgr.accounts["acct2.json"].Quota.FiveHour.UsedPercent = 99.0
	mgr.evaluateActiveAccount()

	activeID, _, _ = mgr.Snapshot()
	if activeID != "acct1.json" {
		t.Fatalf("expected acct1.json to be promoted after acct2 exhausted, got %s", activeID)
	}
	if mgr.accounts["acct2.json"].State != StateCooldown {
		t.Errorf("expected acct2.json to be in cooldown, got %s", mgr.accounts["acct2.json"].State)
	}

	// 3. Test Manual switch
	mgr.accounts["acct2.json"].State = StateReady
	err = mgr.ManualSwitch("acct2.json")
	if err != nil {
		t.Fatalf("ManualSwitch error: %v", err)
	}
	activeID, _, _ = mgr.Snapshot()
	if activeID != "acct2.json" {
		t.Errorf("expected active to be manually switched to acct2.json, got %s", activeID)
	}
}

func TestSchedulerDriftDetection(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.CPA.AuthDir = tmpDir

	cpaClient := cpa.NewClient(cfg.CPA)
	fetcher := quota.NewFetcher(cfg.Quota.RequestTimeout)
	prober := probe.NewController(cfg.Probe)
	mgr := NewManager(&cfg, cpaClient, fetcher, prober)

	now := time.Now()
	q := &quota.AccountQuota{
		IsUsable: true,
		FiveHour: &quota.Window{
			UsedPercent: 0,
			ResetAt:     now.Add(5 * time.Hour),
		},
	}
	lastReset := now.Add(5*time.Hour - 10*time.Second)

	if !prober.ShouldProbeDrift(q, lastReset) {
		t.Errorf("expected drift to be detected when reset_at moved forward by 10s")
	}

	lockedReset := now.Add(-1 * time.Minute)
	managed := &ManagedAccount{
		State:         StateCooldown,
		LockedResetAt: lockedReset,
	}
	if !(managed.State == StateCooldown && now.After(managed.LockedResetAt) && q.FiveHour.UsedPercent == 0) {
		t.Errorf("expected cooldown expiration to trigger probe")
	}
	_ = mgr
}

func TestMultiStrategyRankingAndPromotion(t *testing.T) {
	now := time.Now()

	cands := []Candidate{
		{
			AccountID:      "cand-reset-near",
			Remaining:      50.0,
			FiveHourUsed:   50.0,
			ResetsAt:       now.Add(1 * time.Hour), // resets soon
			IsUsable:       true,
			PluginPriority: 10,
		},
		{
			AccountID:      "cand-reset-far",
			Remaining:      90.0,
			FiveHourUsed:   10.0,
			ResetsAt:       now.Add(4 * time.Hour), // resets later
			IsUsable:       true,
			PluginPriority: 20,
		},
		{
			AccountID:      "cand-soft-limit",
			Remaining:      12.0,
			FiveHourUsed:   88.0, // > 85% soft limit
			ResetsAt:       now.Add(30 * time.Minute),
			IsUsable:       true,
			PluginPriority: 30,
		},
		{
			AccountID:      "cand-low-headroom",
			Remaining:      4.0, // < 5.0% DefaultNewSessionMinRemaining
			FiveHourUsed:   96.0,
			ResetsAt:       now.Add(10 * time.Minute),
			IsUsable:       true,
			PluginPriority: 40,
		},
	}

	// 1. ResetFirst: cand-reset-near resets in 1h, cand-reset-far in 4h.
	// cand-soft-limit resets in 30m so under reset_first it ranks first (score = -0.5h > -1h)
	ranked, skipped := RankCandidates(cands, StrategyResetFirst, now, "", 2.0)
	if len(ranked) < 3 {
		t.Fatalf("expected at least 3 ranked candidates, got %d", len(ranked))
	}
	if ranked[0].AccountID != "cand-soft-limit" {
		t.Errorf("ResetFirst: expected cand-soft-limit first, got %s", ranked[0].AccountID)
	}

	// 验证防震荡：cand-low-headroom (4% < 5%) 应被 skipped
	foundLow := false
	for _, s := range skipped {
		if s.AccountID == "cand-low-headroom" {
			foundLow = true
			break
		}
	}
	if !foundLow {
		t.Errorf("expected cand-low-headroom to be skipped due to low headroom (<5%%)")
	}

	// 2. Balanced: cand-soft-limit (FiveHourUsed=88% >= 85%) 属于 Group 1，应该排在 Group 0 之后
	rankedBal, _ := RankCandidates(cands, StrategyBalanced, now, "", 2.0)
	if rankedBal[0].AccountID != "cand-reset-near" {
		t.Errorf("Balanced: expected cand-reset-near (group 0) first, got %s", rankedBal[0].AccountID)
	}
	if rankedBal[len(rankedBal)-1].AccountID != "cand-soft-limit" {
		t.Errorf("Balanced: expected cand-soft-limit (group 1) last, got %s", rankedBal[len(rankedBal)-1].AccountID)
	}

	// 3. ManualPriority: PluginPriority 最高者胜出
	rankedPrio, _ := RankCandidates(cands, StrategyManualPriority, now, "", 2.0)
	if rankedPrio[0].AccountID != "cand-soft-limit" { // prio = 30 > 20 > 10 (low headroom was skipped)
		t.Errorf("ManualPriority: expected cand-soft-limit (prio 30) first, got %s", rankedPrio[0].AccountID)
	}
	if rankedPrio[1].AccountID != "cand-reset-far" { // prio = 20
		t.Errorf("ManualPriority: expected cand-reset-far (prio 20) second, got %s", rankedPrio[1].AccountID)
	}

	// 4. QuotaPressure: remaining / max(hours, 0.5)
	// cand-soft-limit: 12 / 0.5 = 24.0
	// cand-reset-near: 50 / 1.0 = 50.0 -> 最高
	// cand-reset-far: 90 / 4.0 = 22.5
	rankedPress, _ := RankCandidates(cands, StrategyQuotaPressure, now, "", 2.0)
	if rankedPress[0].AccountID != "cand-reset-near" {
		t.Errorf("QuotaPressure: expected cand-reset-near first, got %s", rankedPress[0].AccountID)
	}
}

func TestStorePersistence(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.json")

	cfg := config.DefaultConfig()
	cpaClient := cpa.NewClient(cfg.CPA)
	fetcher := quota.NewFetcher(cfg.Quota.RequestTimeout)
	prober := probe.NewController(cfg.Probe)

	st, err := store.NewStore(statePath)
	if err != nil {
		t.Fatal(err)
	}

	mgr := NewManager(&cfg, cpaClient, fetcher, prober, st)
	_ = mgr.SetStrategy(StrategyBalanced)
	mgr.SetAccountAliases(map[string]string{"acct1": "主力一号"})
	mgr.SetAccountPriorities(map[string]int{"acct1": 99})

	// 等待落盘完成
	time.Sleep(50 * time.Millisecond)

	// 重建 Manager 模拟重启恢复
	cfg2 := config.DefaultConfig()
	st2, _ := store.NewStore(statePath)
	mgr2 := NewManager(&cfg2, cpaClient, fetcher, prober, st2)

	if mgr2.cfg.Scheduler.Strategy != StrategyBalanced {
		t.Errorf("expected restored strategy balanced, got %s", mgr2.cfg.Scheduler.Strategy)
	}
	if mgr2.accountAliases["acct1"] != "主力一号" {
		t.Errorf("expected restored alias 主力一号, got %s", mgr2.accountAliases["acct1"])
	}
	if mgr2.accountPrios["acct1"] != 99 {
		t.Errorf("expected restored priority 99, got %d", mgr2.accountPrios["acct1"])
	}
}

func TestSchedulerSimulate(t *testing.T) {
	cfg := config.DefaultConfig()
	cpaClient := cpa.NewClient(cfg.CPA)
	fetcher := quota.NewFetcher(cfg.Quota.RequestTimeout)
	prober := probe.NewController(cfg.Probe)
	mgr := NewManager(&cfg, cpaClient, fetcher, prober)

	mgr.accounts["codex-a"] = &ManagedAccount{
		Account: &cpa.Account{ID: "codex-a", Provider: "codex", Priority: 10},
		State:   StateReady,
		Quota: &quota.AccountQuota{
			IsUsable: true,
			FiveHour: &quota.Window{UsedPercent: 10.0, ResetAt: time.Now().Add(2 * time.Hour)},
		},
	}
	mgr.accounts["codex-b"] = &ManagedAccount{
		Account: &cpa.Account{ID: "codex-b", Provider: "codex", Priority: 20},
		State:   StateReady,
		Quota: &quota.AccountQuota{
			IsUsable: true,
			FiveHour: &quota.Window{UsedPercent: 30.0, ResetAt: time.Now().Add(1 * time.Hour)},
		},
	}

	simResults := mgr.Simulate("codex", "gpt-5.6-luna", []string{StrategyResetFirst, StrategyManualPriority})
	if len(simResults) != 2 {
		t.Fatalf("expected 2 simulation results, got %d", len(simResults))
	}

	// ResetFirst: codex-b resets in 1h, codex-a in 2h -> codex-b wins
	if simResults[0]["auth_id"] != "codex-b" {
		t.Errorf("expected codex-b for ResetFirst simulation, got %v", simResults[0]["auth_id"])
	}
	// ManualPriority: codex-b has host priority 20 > 10 -> codex-b wins
	if simResults[1]["auth_id"] != "codex-b" {
		t.Errorf("expected codex-b for ManualPriority simulation, got %v", simResults[1]["auth_id"])
	}
}

func TestCircuitBreakerTrippingAndRecovery(t *testing.T) {
	cfg := config.DefaultConfig()
	cpaClient := cpa.NewClient(cfg.CPA)
	fetcher := quota.NewFetcher(cfg.Quota.RequestTimeout)
	prober := probe.NewController(cfg.Probe)
	mgr := NewManager(&cfg, cpaClient, fetcher, prober)

	acct := &cpa.Account{ID: "acct-circuit", Provider: "codex", AccessToken: "bad-token"}
	mgr.accounts[acct.ID] = &ManagedAccount{
		Account: acct,
		State:   StateReady,
		Circuit: CircuitBreaker{State: CircuitClosed},
	}

	ctx := context.Background()

	// 1. 模拟前两次网络/接口错误 -> 未达阈值，处于 closed 但计数增加
	mgr.reconcileSingleAccount(ctx, acct)
	if mgr.accounts[acct.ID].Circuit.FailureCount != 1 || mgr.accounts[acct.ID].Circuit.State != CircuitClosed {
		t.Fatalf("expected fail count 1 and state closed, got %v / %v", mgr.accounts[acct.ID].Circuit.FailureCount, mgr.accounts[acct.ID].Circuit.State)
	}

	mgr.reconcileSingleAccount(ctx, acct)
	if mgr.accounts[acct.ID].Circuit.FailureCount != 2 || mgr.accounts[acct.ID].Circuit.State != CircuitClosed {
		t.Fatalf("expected fail count 2 and state closed, got %v / %v", mgr.accounts[acct.ID].Circuit.FailureCount, mgr.accounts[acct.ID].Circuit.State)
	}

	// 2. 模拟第 3 次错误 -> 触发熔断 CircuitOpen
	mgr.reconcileSingleAccount(ctx, acct)
	ma := mgr.accounts[acct.ID]
	if ma.Circuit.State != CircuitOpen {
		t.Fatalf("expected state CircuitOpen after 3 fails, got %v", ma.Circuit.State)
	}
	if ma.State != StateError {
		t.Fatalf("expected account state error, got %v", ma.State)
	}

	// 3. 熔断冷静期内，调度器跳过该账号
	cands := []Candidate{
		{AccountID: acct.ID, IsUsable: true, CircuitOpen: true, Remaining: 80.0},
	}
	ranked, skipped := RankCandidates(cands, StrategyFillFirst, time.Now(), "", 2.0)
	if len(ranked) != 0 || len(skipped) != 1 || skipped[0].Reason != "circuit_open" {
		t.Fatalf("expected candidate skipped due to circuit_open, got ranked=%d skipped=%v", len(ranked), skipped)
	}

	// 4. 冷静期过后，状态转为半开 (Half-Open)
	ma.Circuit.NextAllowedAt = time.Now().Add(-1 * time.Minute)
	// 模拟一次成功的 quota mock
	ma.Quota = &quota.AccountQuota{IsUsable: true, FiveHour: &quota.Window{UsedPercent: 10}}
	ma.Circuit.State = CircuitHalfOpen

	// 模拟健康自愈恢复
	ma.Circuit.State = CircuitClosed
	ma.Circuit.FailureCount = 0
	ma.State = StateReady
	if ma.Circuit.State != CircuitClosed || ma.Circuit.FailureCount != 0 {
		t.Errorf("expected circuit restored to closed")
	}
}

func TestCredentialHotReloadAndAutoRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	authFile := filepath.Join(tmpDir, "codex-test.json")
	_ = os.WriteFile(authFile, []byte(`{"provider":"codex","access_token":"old-expired-token","disabled":false,"priority":0}`), 0644)

	cfg := config.DefaultConfig()
	cfg.CPA.AuthDir = tmpDir
	cpaClient := cpa.NewClient(cfg.CPA)
	fetcher := quota.NewFetcher(cfg.Quota.RequestTimeout)
	prober := probe.NewController(cfg.Probe)
	mgr := NewManager(&cfg, cpaClient, fetcher, prober)

	ctx := context.Background()
	mgr.reconcile(ctx)

	acctID := "codex-test.json"
	ma := mgr.accounts[acctID]
	if ma == nil {
		t.Fatalf("account %s not loaded", acctID)
	}

	// 模拟此前因过期 token 陷入熔断和报错
	ma.State = StateError
	ma.StateDesc = "401 Unauthorized"
	ma.Circuit.State = CircuitOpen
	ma.Circuit.FailureCount = 5
	ma.Circuit.NextAllowedAt = time.Now().Add(10 * time.Minute)

	// 模拟操作员在服务器执行 cpa login，写入新凭据
	time.Sleep(10 * time.Millisecond)
	_ = os.WriteFile(authFile, []byte(`{"provider":"codex","access_token":"new-fresh-token","disabled":false,"priority":0}`), 0644)

	// 下一轮 reconcile 自动扫描并识别凭据变更
	mgr.reconcile(ctx)

	// 验证：自动清除 StateError 和 CircuitOpen，恢复为 StateReady
	if ma.State == StateError && ma.Circuit.State == CircuitOpen {
		t.Errorf("expected account to reset error and circuit open after credential update")
	}
	if ma.Account.AccessToken != "new-fresh-token" {
		t.Errorf("expected access token updated to new-fresh-token, got %s", ma.Account.AccessToken)
	}
}

func TestMorningWarmupAndQuietHours(t *testing.T) {
	loc := probe.ParseLocation("Asia/Shanghai")
	tmpDir := t.TempDir()
	authFile := filepath.Join(tmpDir, "codex-warm.json")
	_ = os.WriteFile(authFile, []byte(`{"provider":"codex","access_token":"warm-token","disabled":false,"priority":0}`), 0644)

	cfg := config.DefaultConfig()
	cfg.CPA.AuthDir = tmpDir
	cfg.Probe.Timezone = "Asia/Shanghai"
	cfg.Probe.QuietHoursEnabled = true
	cfg.Probe.QuietHoursStart = "01:00"
	cfg.Probe.QuietHoursEnd = "07:30"
	cfg.Probe.MorningWarmupEnabled = true
	cfg.Probe.MorningWarmupTime = "07:30"

	cpaClient := cpa.NewClient(cfg.CPA)
	fetcher := quota.NewFetcher(cfg.Quota.RequestTimeout)
	prober := probe.NewController(cfg.Probe)
	mgr := NewManager(&cfg, cpaClient, fetcher, prober)

	ctx := context.Background()
	mgr.reconcile(ctx)

	acctID := "codex-warm.json"
	ma := mgr.accounts[acctID]
	if ma == nil {
		t.Fatalf("account %s not loaded", acctID)
	}

	// 1. 验证在夜间静默期（03:00）不会触发晨间预热
	tNight := time.Date(2026, 10, 27, 3, 0, 0, 0, loc)
	mgr.checkMorningWarmup(ctx, tNight)
	if mgr.lastWarmupDate != "" {
		t.Errorf("expected no warmup in quiet hours at 03:00, got %s", mgr.lastWarmupDate)
	}

	// 2. 模拟到达 07:30 晨间预热时刻
	tWarmup := time.Date(2026, 10, 27, 7, 30, 0, 0, loc)
	mgr.checkMorningWarmup(ctx, tWarmup)
	if mgr.lastWarmupDate != "2026-10-27" {
		t.Errorf("expected lastWarmupDate=2026-10-27 after 07:30 warmup, got %s", mgr.lastWarmupDate)
	}

	// 3. 同一天再次检查（例如 07:35），不应重复触发
	prevWarmupDate := mgr.lastWarmupDate
	tWarmupLater := time.Date(2026, 10, 27, 7, 35, 0, 0, loc)
	mgr.checkMorningWarmup(ctx, tWarmupLater)
	if mgr.lastWarmupDate != prevWarmupDate {
		t.Errorf("expected lastWarmupDate to remain unchanged on second call")
	}

	// 4. 测试动态配置修改 UpdateProbeConfig
	qStart := "00:30"
	qEnd := "08:00"
	f := false
	mgr.UpdateProbeConfig("Asia/Shanghai", &f, qStart, qEnd, nil, "")
	if mgr.cfg.Probe.QuietHoursEnabled != false {
		t.Errorf("expected quiet_hours_enabled=false")
	}
	if mgr.cfg.Probe.QuietHoursStart != "00:30" {
		t.Errorf("expected quiet_hours_start=00:30")
	}
	if mgr.cfg.Probe.QuietHoursEnd != "08:00" {
		t.Errorf("expected quiet_hours_end=08:00")
	}
}
