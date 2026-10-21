package probe

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cuota-server/config"
	"cuota-server/cpa"
	"cuota-server/quota"
)

func TestShouldProbe(t *testing.T) {
	ctrl := NewController(config.ProbeConfig{
		Enabled: true,
	})

	now := time.Now()

	// 1. 0% 使用率且 reset_at 为空 -> 应该探测激活
	q1 := &quota.AccountQuota{
		IsUsable: true,
		FiveHour: &quota.Window{
			UsedPercent: 0,
		},
	}
	if !ctrl.ShouldProbe(q1) {
		t.Errorf("expected ShouldProbe=true for 0%% used with zero reset_at")
	}

	// 2. reset_at 在过去 -> 应该探测激活
	q2 := &quota.AccountQuota{
		IsUsable: true,
		FiveHour: &quota.Window{
			UsedPercent: 90,
			ResetAt:     now.Add(-5 * time.Minute),
		},
	}
	if !ctrl.ShouldProbe(q2) {
		t.Errorf("expected ShouldProbe=true when reset_at passed")
	}

	// 3. reset_at 在未来 -> 不应该探测
	q3 := &quota.AccountQuota{
		IsUsable: true,
		FiveHour: &quota.Window{
			UsedPercent: 50,
			ResetAt:     now.Add(2 * time.Hour),
		},
	}
	if ctrl.ShouldProbe(q3) {
		t.Errorf("expected ShouldProbe=false when reset_at is in future")
	}
}

func TestExecuteProbe(t *testing.T) {
	var receivedModel string
	var authHeader string

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if m, ok := body["model"].(string); ok {
			receivedModel = m
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"response":{"usage":{"total_tokens":2}}}`))
	}))
	defer mockServer.Close()

	ctrl := NewController(config.ProbeConfig{
		Enabled:  true,
		Model:    "gpt-5.6-luna",
		Endpoint: mockServer.URL,
	})

	acct := &cpa.Account{
		ID:          "test-acct",
		AccessToken: "test-token-123",
	}

	ev, err := ctrl.Execute(context.Background(), acct)
	if err != nil {
		t.Fatalf("unexpected error executing probe: %v", err)
	}

	if !ev.Success {
		t.Errorf("expected probe event to succeed, got %v", ev)
	}
	if receivedModel != "gpt-5.6-luna" {
		t.Errorf("expected model gpt-5.6-luna, got '%s'", receivedModel)
	}
	if authHeader != "Bearer test-token-123" {
		t.Errorf("expected Authorization Bearer test-token-123, got '%s'", authHeader)
	}

	history := ctrl.History()
	if len(history) != 1 {
		t.Errorf("expected 1 history event, got %d", len(history))
	}
}
