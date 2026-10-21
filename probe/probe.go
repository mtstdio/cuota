package probe

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
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

type Controller struct {
	cfg        config.ProbeConfig
	httpClient *http.Client
	mu         sync.RWMutex
	history    []Event
	maxHistory int
	probing    map[string]bool
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
	}
}

func (c *Controller) ShouldProbe(q *quota.AccountQuota) bool {
	if !c.cfg.Enabled || q == nil || !q.IsUsable {
		return false
	}
	if q.FiveHour == nil {
		return false
	}

	now := time.Now()

	// 1. 如果 reset_at 已经过去，说明 5h 周期结束需要触发激活
	if !q.FiveHour.ResetAt.IsZero() && now.After(q.FiveHour.ResetAt) {
		return true
	}

	// 2. 如果 5h 额度使用率为 0，且没有重置倒计时或倒计时未生效，触发激活启动倒计时
	if q.FiveHour.UsedPercent == 0 && q.FiveHour.ResetAt.IsZero() {
		return true
	}

	return false
}

func (c *Controller) Execute(ctx context.Context, account *cpa.Account) (*Event, error) {
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

	// 消费部分响应体
	buf := make([]byte, 2048)
	n, _ := io.ReadAtLeast(resp.Body, buf, 1)
	bodySample := string(buf[:n])

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		event.Success = true
		event.Message = fmt.Sprintf("5h window activated successfully (status %d)", resp.StatusCode)
		log.Printf("[Probe] Account %s activated successfully in %v!", account.ID, event.Duration)
	} else {
		event.Success = false
		event.Message = fmt.Sprintf("probe rejected with status %d: %s", resp.StatusCode, bodySample)
		log.Printf("[Probe] Account %s probe failed with status %d: %s", account.ID, resp.StatusCode, bodySample)
	}

	c.recordEvent(*event)
	return event, nil
}

func (c *Controller) recordEvent(ev Event) {
	c.mu.Lock()
	defer c.mu.Unlock()

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
