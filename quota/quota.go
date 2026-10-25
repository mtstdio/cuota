package quota

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"cuota-server/cpa"
)

const (
	codexUsageEndpoint = "https://chatgpt.com/backend-api/wham/usage"
	defaultUserAgent   = "codex_cli_rs/0.76.0 (Debian 13.0.0; x86_64) WindowsTerminal"
)

type Window struct {
	WindowSeconds int64     `json:"window_seconds"` // 18000 for 5h, 604800 for weekly
	UsedPercent   float64   `json:"used_percent"`   // 0.0 - 100.0
	ResetAt       time.Time `json:"reset_at"`
}

type AccountQuota struct {
	AccountID  string    `json:"account_id"`
	PlanType   string    `json:"plan_type"`
	FiveHour   *Window   `json:"five_hour,omitempty"`
	Weekly     *Window   `json:"weekly,omitempty"`
	FetchedAt  time.Time `json:"fetched_at"`
	IsUsable   bool      `json:"is_usable"`
	Error      string    `json:"error,omitempty"`
	StatusCode int       `json:"status_code,omitempty"`
}

type Fetcher struct {
	client *http.Client
}

func NewFetcher(timeout time.Duration) *Fetcher {
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	return &Fetcher{
		client: &http.Client{Timeout: timeout},
	}
}

func (f *Fetcher) Fetch(account *cpa.Account) (*AccountQuota, error) {
	if account.Provider == "claude" {
		return f.FetchClaude(account)
	}

	now := time.Now()
	result := &AccountQuota{
		AccountID: account.ID,
		FetchedAt: now,
		IsUsable:  false,
	}

	if account.AccessToken == "" {
		result.Error = "missing access token"
		return result, fmt.Errorf("missing access token for account %s", account.ID)
	}

	req, err := http.NewRequest(http.MethodGet, codexUsageEndpoint, nil)
	if err != nil {
		result.Error = err.Error()
		return result, err
	}

	req.Header.Set("Authorization", "Bearer "+account.AccessToken)
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Accept", "application/json")
	if account.ChatGPTAccountID != "" {
		req.Header.Set("Chatgpt-Account-Id", account.ChatGPTAccountID)
	}

	resp, err := f.client.Do(req)
	if err != nil {
		result.Error = err.Error()
		return result, err
	}
	defer resp.Body.Close()

	result.StatusCode = resp.StatusCode
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		result.Error = fmt.Sprintf("status %d: %s", resp.StatusCode, string(body))
		return result, fmt.Errorf("quota endpoint returned %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		result.Error = err.Error()
		return result, err
	}

	if err := parseUsageResponse(body, now, result); err != nil {
		result.Error = err.Error()
		return result, err
	}

	result.IsUsable = true
	return result, nil
}

func parseUsageResponse(raw []byte, now time.Time, out *AccountQuota) error {
	var doc struct {
		PlanType  string `json:"plan_type"`
		RateLimit struct {
			Allowed         *bool           `json:"allowed"`
			LimitReached    *bool           `json:"limit_reached"`
			PrimaryWindow   json.RawMessage `json:"primary_window"`
			SecondaryWindow json.RawMessage `json:"secondary_window"`
		} `json:"rate_limit"`
	}

	if err := json.Unmarshal(raw, &doc); err != nil {
		return fmt.Errorf("decode usage json: %w", err)
	}

	out.PlanType = doc.PlanType
	isLimitReached := false
	if doc.RateLimit.LimitReached != nil && *doc.RateLimit.LimitReached {
		isLimitReached = true
	}
	if doc.RateLimit.Allowed != nil && !*doc.RateLimit.Allowed {
		isLimitReached = true
	}

	if len(doc.RateLimit.PrimaryWindow) > 0 {
		w := parseWindow(doc.RateLimit.PrimaryWindow, now, 18000)
		if w != nil {
			if isLimitReached {
				w.UsedPercent = 100.0
			}
			out.FiveHour = w
		}
	}

	if len(doc.RateLimit.SecondaryWindow) > 0 {
		w := parseWindow(doc.RateLimit.SecondaryWindow, now, 604800)
		if w != nil {
			if isLimitReached {
				w.UsedPercent = 100.0
			}
			out.Weekly = w
		}
	}

	return nil
}

func parseWindow(raw json.RawMessage, now time.Time, defaultSeconds int64) *Window {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil
	}

	used := 0.0
	if u, ok := m["used_percent"].(float64); ok {
		used = u
	} else if u, ok := m["usedPercent"].(float64); ok {
		used = u
	}

	windowSecs := defaultSeconds
	if s, ok := m["limit_window_seconds"].(float64); ok && s > 0 {
		windowSecs = int64(s)
	}

	var resetAt time.Time
	if rSec, ok := m["reset_after_seconds"].(float64); ok && rSec > 0 {
		resetAt = now.Add(time.Duration(rSec) * time.Second)
	} else if rEpoch, ok := m["reset_at"].(float64); ok && rEpoch > 0 {
		resetAt = time.Unix(int64(rEpoch), 0)
	} else if rStr, ok := m["reset_at"].(string); ok && rStr != "" {
		if t, err := time.Parse(time.RFC3339, rStr); err == nil {
			resetAt = t
		}
	}

	return &Window{
		WindowSeconds: windowSecs,
		UsedPercent:   used,
		ResetAt:       resetAt,
	}
}
