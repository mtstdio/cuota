package cpa

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"cuota-server/config"
)

func TestClientLocalAuthDir(t *testing.T) {
	tmpDir := t.TempDir()

	file1 := filepath.Join(tmpDir, "codex-1.json")
	file2 := filepath.Join(tmpDir, "codex-2.json")

	data1 := []byte(`{"access_token":"t1","account_id":"acc1","disabled":false,"priority":0}`)
	data2 := []byte(`{"access_token":"t2","account_id":"acc2","disabled":true,"priority":10}`)

	if err := os.WriteFile(file1, data1, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file2, data2, 0644); err != nil {
		t.Fatal(err)
	}

	cfg := config.CPAConfig{
		AuthDir: tmpDir,
	}
	client := NewClient(cfg)

	accounts, err := client.LoadAccounts()
	if err != nil {
		t.Fatalf("LoadAccounts error: %v", err)
	}

	if len(accounts) != 2 {
		t.Fatalf("expected 2 accounts, got %d", len(accounts))
	}

	// Update status
	err = client.UpdateAccountStatus("codex-1.json", true, 100)
	if err != nil {
		t.Fatalf("UpdateAccountStatus error: %v", err)
	}

	acct, ok := client.GetAccount("codex-1.json")
	if !ok {
		t.Fatalf("account codex-1.json not found in memory")
	}
	if !acct.Disabled || acct.Priority != 100 {
		t.Errorf("expected disabled=true, priority=100, got %v, %d", acct.Disabled, acct.Priority)
	}

	// Verify disk file
	updatedDiskBytes, err := os.ReadFile(file1)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(updatedDiskBytes, &doc); err != nil {
		t.Fatal(err)
	}
	if doc["disabled"] != true || doc["priority"] != float64(100) {
		t.Errorf("disk json not properly updated: %s", string(updatedDiskBytes))
	}
}
