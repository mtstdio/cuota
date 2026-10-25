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

func TestShouldProbeDrift(t *testing.T) {
	ctrl := NewController(config.ProbeConfig{
		Enabled: true,
	})

	now := time.Now()

	// 1. 0% 使用率且 reset_at 向后漂移 -> 应该探测激活 (Lazy Window)
	q1 := &quota.AccountQuota{
		IsUsable: true,
		FiveHour: &quota.Window{
			UsedPercent: 0,
			ResetAt:     now.Add(5 * time.Hour),
		},
	}
	lastReset1 := now.Add(5*time.Hour - 10*time.Second) // 漂移了 10 秒
	if !ctrl.ShouldProbeDrift(q1, lastReset1) {
		t.Errorf("expected ShouldProbeDrift=true when reset_at drifted forward")
	}

	// 2. 0% 使用率但 reset_at 未漂移（已锁定倒计时） -> 不应该探测
	lastReset2 := q1.FiveHour.ResetAt
	if ctrl.ShouldProbeDrift(q1, lastReset2) {
		t.Errorf("expected ShouldProbeDrift=false when reset_at did not drift")
	}

	// 3. 有使用量（>0%） -> 不应该探测
	q2 := &quota.AccountQuota{
		IsUsable: true,
		FiveHour: &quota.Window{
			UsedPercent: 10,
			ResetAt:     now.Add(5 * time.Hour),
		},
	}
	if ctrl.ShouldProbeDrift(q2, lastReset1) {
		t.Errorf("expected ShouldProbeDrift=false when used > 0")
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

func TestExecuteProbeSSE(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("event: response.created\ndata: {\"type\":\"response.created\"}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"total_tokens\":12}}}\n\n"))
	}))
	defer mockServer.Close()

	ctrl := NewController(config.ProbeConfig{
		Enabled:  true,
		Model:    "gpt-5.6-luna",
		Endpoint: mockServer.URL,
	})

	acct := &cpa.Account{
		ID:          "test-acct-sse",
		AccessToken: "test-token-456",
	}

	ev, err := ctrl.Execute(context.Background(), acct)
	if err != nil {
		t.Fatalf("unexpected error executing probe: %v", err)
	}

	if !ev.Success {
		t.Errorf("expected probe event to succeed, got %v", ev)
	}
	if ev.Message != "5h 额度激活成功，实际消耗 12 tokens" {
		t.Errorf("expected tokens message, got '%s'", ev.Message)
	}
}

func TestProbeSuppressionAndBackoff(t *testing.T) {
	statusToReturn := http.StatusOK
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(statusToReturn)
		if statusToReturn == http.StatusOK {
			_, _ = w.Write([]byte(`{"response":{"usage":{"total_tokens":5}}}`))
		} else {
			_, _ = w.Write([]byte(`error`))
		}
	}))
	defer mockServer.Close()

	ctrl := NewController(config.ProbeConfig{
		Enabled:  true,
		Endpoint: mockServer.URL,
	})

	acct := &cpa.Account{
		ID:          "suppress-acct",
		AccessToken: "dummy-token",
	}

	// 1. 首次探测成功
	ev, err := ctrl.Execute(context.Background(), acct)
	if err != nil || !ev.Success {
		t.Fatalf("first probe failed: %v", err)
	}

	// 此时应进入 10m 抑制期
	suppressed, until := ctrl.IsSuppressed(acct.ID, time.Now())
	if !suppressed {
		t.Errorf("expected account to be suppressed after success")
	}
	if until.Before(time.Now().Add(9 * time.Minute)) {
		t.Errorf("expected suppress until ~10m from now, got %v", until)
	}

	// 2. 抑制期内自动执行应被拦截
	_, err = ctrl.Execute(context.Background(), acct)
	if err == nil {
		t.Errorf("expected suppressed probe Execute to return error")
	}

	// 3. ShouldProbe / ShouldProbeDrift 应直接返回 false
	q := &quota.AccountQuota{
		AccountID: acct.ID,
		IsUsable:  true,
		FiveHour: &quota.Window{
			UsedPercent: 0,
			ResetAt:     time.Now().Add(-time.Minute),
		},
	}
	if ctrl.ShouldProbe(q) {
		t.Errorf("expected ShouldProbe=false while suppressed")
	}
	if ctrl.ShouldProbeDrift(q, time.Now().Add(-2*time.Minute)) {
		t.Errorf("expected ShouldProbeDrift=false while suppressed")
	}

	// 4. ExecuteForced 允许绕过抑制
	forcedEv, err := ctrl.ExecuteForced(context.Background(), acct)
	if err != nil || !forcedEv.Success {
		t.Errorf("ExecuteForced should succeed even during suppression: %v", err)
	}

	// 5. 模拟失败与指数退避
	statusToReturn = http.StatusInternalServerError
	_, _ = ctrl.ExecuteForced(context.Background(), acct) // fail 1 -> 10m
	_, _ = ctrl.ExecuteForced(context.Background(), acct) // fail 2 -> 20m
	_, until2 := ctrl.IsSuppressed(acct.ID, time.Now())
	if until2.Before(time.Now().Add(18 * time.Minute)) {
		t.Errorf("expected backoff ~20m after 2 fails, got %v", until2)
	}
}
