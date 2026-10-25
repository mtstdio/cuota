package scheduler

import (
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
