package probe

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"cuota-server/config"
	"cuota-server/cpa"
	"cuota-server/quota"
)

type Event struct {
	ID        string        `json:"id"`
	AccountID string        `json:"account_id"`
	Timestamp time.Time     `json:"timestamp"`
	Success   bool          `json:"success"`
	Status    int           `json:"status"`
	Duration  time.Duration `json:"duration_ms"`
	Message   string        `json:"message"`
}

type AccountProbeState struct {
	SuppressUntil    time.Time `json:"suppress_until"`
	ConsecutiveFails int       `json:"consecutive_fails"`
	LastAttemptAt    time.Time `json:"last_attempt_at"`
}

type Controller struct {
	cfg        config.ProbeConfig
	httpClient *http.Client
	mu         sync.RWMutex
	history    []Event
	maxHistory int
	probing    map[string]bool
	states     map[string]*AccountProbeState
}

func NewController(cfg config.ProbeConfig) *Controller {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &Controller{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		maxHistory: 100,
		probing:    make(map[string]bool),
		states:     make(map[string]*AccountProbeState),
	}
}

func (c *Controller) IsSuppressed(accountID string, now time.Time) (bool, time.Time) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if st, ok := c.states[accountID]; ok {
		if now.Before(st.SuppressUntil) {
			return true, st.SuppressUntil
		}
	}
	return false, time.Time{}
}

func (c *Controller) updateSuppressionLocked(accountID string, success bool, now time.Time) {
	st, ok := c.states[accountID]
	if !ok {
		st = &AccountProbeState{}
		c.states[accountID] = st
	}
	st.LastAttemptAt = now
	if success {
		st.ConsecutiveFails = 0
		st.SuppressUntil = now.Add(10 * time.Minute)
	} else {
		st.ConsecutiveFails++
		multiplier := 1 << (st.ConsecutiveFails - 1)
		if multiplier > 6 {
			multiplier = 6
		}
		st.SuppressUntil = now.Add(time.Duration(multiplier) * 10 * time.Minute)
	}
}

func (c *Controller) UpdateConfig(cfg config.ProbeConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cfg = cfg
}

func (c *Controller) Config() config.ProbeConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cfg
}

// ParseTimeMinutes parses a "HH:MM" string into minutes of the day (0-1439).
func ParseTimeMinutes(s string) (int, error) {
	parts := strings.Split(strings.TrimSpace(s), ":")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid time format: %q", s)
	}
	h, err := strconv.Atoi(parts[0])
	if err != nil || h < 0 || h > 23 {
		return 0, fmt.Errorf("invalid hour: %q", parts[0])
	}
	m, err := strconv.Atoi(parts[1])
	if err != nil || m < 0 || m > 59 {
		return 0, fmt.Errorf("invalid minute: %q", parts[1])
	}
	return h*60 + m, nil
}

// ParseLocation parses timezone name, fallback to Asia/Shanghai (+8)
func ParseLocation(tz string) *time.Location {
	if strings.TrimSpace(tz) == "" {
		return time.FixedZone("CST", 8*3600)
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return time.FixedZone("CST", 8*3600)
	}
	return loc
}

func (c *Controller) IsInQuietHours(t time.Time) bool {
	c.mu.RLock()
	cfg := c.cfg
	c.mu.RUnlock()

	if !cfg.QuietHoursEnabled {
		return false
	}

	startMin, err1 := ParseTimeMinutes(cfg.QuietHoursStart)
	endMin, err2 := ParseTimeMinutes(cfg.QuietHoursEnd)
	if err1 != nil || err2 != nil {
		return false
	}

	loc := ParseLocation(cfg.Timezone)
	localT := t.In(loc)
	curMin := localT.Hour()*60 + localT.Minute()

	if startMin < endMin {
		return curMin >= startMin && curMin < endMin
	} else if startMin > endMin {
		// 跨午夜情况，如 23:00 - 07:30
		return curMin >= startMin || curMin < endMin
	}
	return false
}

func (c *Controller) ShouldProbeDrift(q *quota.AccountQuota, lastResetAt time.Time) bool {
	if !c.cfg.Enabled || q == nil || !q.IsUsable || q.FiveHour == nil {
		return false
	}

	now := time.Now()
	if c.IsInQuietHours(now) {
		return false
	}

	if suppressed, _ := c.IsSuppressed(q.AccountID, now); suppressed {
		return false
	}

	// 使用率为 0，且上次观测的 ResetAt 存在，且当前 ResetAt 发生了向后漂移（说明倒计时未锁定）
	if q.FiveHour.UsedPercent == 0 && !lastResetAt.IsZero() {
		if q.FiveHour.ResetAt.Sub(lastResetAt) >= 3*time.Second {
			return true
		}
	}

	return false
}

func (c *Controller) ShouldProbe(q *quota.AccountQuota) bool {
	if !c.cfg.Enabled || q == nil || !q.IsUsable || q.FiveHour == nil {
		return false
	}

	now := time.Now()
	if c.IsInQuietHours(now) {
		return false
	}

	if suppressed, _ := c.IsSuppressed(q.AccountID, now); suppressed {
		return false
	}

	// reset_at 已经过去，说明 5h 周期结束需要触发激活
	if !q.FiveHour.ResetAt.IsZero() && now.After(q.FiveHour.ResetAt) {
		return true
	}

	return false
}

func (c *Controller) Execute(ctx context.Context, account *cpa.Account) (*Event, error) {
	return c.executeInternal(ctx, account, false)
}

func (c *Controller) ExecuteForced(ctx context.Context, account *cpa.Account) (*Event, error) {
	return c.executeInternal(ctx, account, true)
}

func (c *Controller) executeInternal(ctx context.Context, account *cpa.Account, force bool) (*Event, error) {
	now := time.Now()
	if !force {
		if suppressed, until := c.IsSuppressed(account.ID, now); suppressed {
			return nil, fmt.Errorf("account %s probe suppressed until %s", account.ID, until.Format(time.RFC3339))
		}
	}
	c.mu.Lock()
	if c.probing[account.ID] {
		c.mu.Unlock()
		return nil, fmt.Errorf("account %s is already being probed", account.ID)
	}
	c.probing[account.ID] = true
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.probing, account.ID)
		c.mu.Unlock()
	}()

	start := time.Now()
	event := &Event{
		ID:        fmt.Sprintf("probe-%d", start.UnixNano()),
		AccountID: account.ID,
		Timestamp: start,
	}

	model := c.cfg.Model
	if model == "" {
		model = "gpt-5.6-luna"
	}
	endpoint := c.cfg.Endpoint
	if endpoint == "" {
		endpoint = "https://chatgpt.com/backend-api/codex/responses"
	}

	payload := map[string]any{
		"model":        model,
		"instructions": "",
		"input": []map[string]any{
			{
				"type": "message",
				"role": "user",
				"content": []map[string]any{
					{
						"type": "input_text",
						"text": "ping",
					},
				},
			},
		},
		"stream": true,
		"store":  false,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		event.Success = false
		event.Message = fmt.Sprintf("marshal payload error: %v", err)
		c.recordEvent(*event)
		return event, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		event.Success = false
		event.Message = fmt.Sprintf("create request error: %v", err)
		c.recordEvent(*event)
		return event, err
	}

	req.Header.Set("Authorization", "Bearer "+account.AccessToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "codex_cli_rs/0.76.0 (Debian 13.0.0; x86_64) WindowsTerminal")
	req.Header.Set("Accept", "text/event-stream, application/json")
	if account.ChatGPTAccountID != "" {
		req.Header.Set("Chatgpt-Account-Id", account.ChatGPTAccountID)
	}

	log.Printf("[Probe] Sending 5h activation probe for account %s (model: %s)...", account.ID, model)

	resp, err := c.httpClient.Do(req)
	event.Duration = time.Since(start)
	if err != nil {
		event.Success = false
		event.Message = fmt.Sprintf("network error: %v", err)
		c.recordEvent(*event)
		return event, err
	}
	defer resp.Body.Close()

	event.Status = resp.StatusCode

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		tokens, hasTokens := parseUsageTokens(body)
		if hasTokens {
			event.Success = true
			event.Message = fmt.Sprintf("5h 额度激活成功，实际消耗 %d tokens", tokens)
			log.Printf("[Probe] Account %s activated successfully in %v (tokens: %d)!", account.ID, event.Duration, tokens)
		} else {
			event.Success = true
			event.Message = fmt.Sprintf("5h 探活响应成功 (status %d)", resp.StatusCode)
			log.Printf("[Probe] Account %s probe returned status %d in %v!", account.ID, resp.StatusCode, event.Duration)
		}
	} else {
		event.Success = false
		event.Message = fmt.Sprintf("probe rejected with status %d: %s", resp.StatusCode, string(body))
		log.Printf("[Probe] Account %s probe failed with status %d: %s", account.ID, resp.StatusCode, string(body))
	}

	c.recordEvent(*event)
	return event, nil
}

func parseUsageTokens(body []byte) (int, bool) {
	// 1. 如果是标准 JSON
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err == nil {
		if t, ok := extractTokensFromDoc(doc); ok {
			return t, true
		}
	}

	// 2. 如果是 SSE (text/event-stream)
	lines := bytes.Split(body, []byte("\n"))
	for _, line := range lines {
		line = bytes.TrimSpace(line)
		if bytes.HasPrefix(line, []byte("data: ")) {
			payload := bytes.TrimPrefix(line, []byte("data: "))
			var sseDoc map[string]any
			if err := json.Unmarshal(payload, &sseDoc); err == nil {
				if t, ok := extractTokensFromDoc(sseDoc); ok {
					return t, true
				}
			}
		}
	}
	return 0, false
}

func extractTokensFromDoc(doc map[string]any) (int, bool) {
	paths := [][]string{
		{"usage", "total_tokens"},
		{"usage", "input_tokens"},
		{"response", "usage", "total_tokens"},
		{"response", "usage", "input_tokens"},
	}
	for _, path := range paths {
		var cur any = doc
		ok := true
		for _, key := range path {
			m, isMap := cur.(map[string]any)
			if !isMap {
				ok = false
				break
			}
			cur = m[key]
		}
		if ok {
			if v, isFloat := cur.(float64); isFloat && v > 0 {
				return int(v), true
			}
		}
	}
	return 0, false
}

func (c *Controller) recordEvent(ev Event) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.updateSuppressionLocked(ev.AccountID, ev.Success, ev.Timestamp)

	c.history = append([]Event{ev}, c.history...)
	if len(c.history) > c.maxHistory {
		c.history = c.history[:c.maxHistory]
	}
}

func (c *Controller) History() []Event {
	c.mu.RLock()
	defer c.mu.RUnlock()

	res := make([]Event, len(c.history))
	copy(res, c.history)
	return res
}
