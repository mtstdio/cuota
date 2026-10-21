package scheduler

import (
	"os"
	"path/filepath"
	"testing"

	"cuota-server/config"
	"cuota-server/cpa"
	"cuota-server/probe"
	"cuota-server/quota"
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
