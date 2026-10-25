package quota

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"cuota-server/cpa"
)

const (
	claudeUsageEndpoint = "https://api.anthropic.com/api/oauth/usage"
	claudeAPIVersion    = "2023-06-01"
	claudeUserAgent     = "claude-code/1.0.0 (darwin; x86_64)"
)

type claudeWindowRaw struct {
	Utilization *float64   `json:"utilization"`
	ResetsAt    *time.Time `json:"resets_at"`
}

// FetchClaude 从 Anthropic 官方 OAuth usage 接口拉取配额
func (f *Fetcher) FetchClaude(account *cpa.Account) (*AccountQuota, error) {
	now := time.Now()
	result := &AccountQuota{
		AccountID: account.ID,
		PlanType:  "claude_pro",
		FetchedAt: now,
		IsUsable:  false,
	}

	if account.AccessToken == "" {
		result.Error = "missing access token"
		return result, fmt.Errorf("missing access token for claude account %s", account.ID)
	}

	req, err := http.NewRequest(http.MethodGet, claudeUsageEndpoint, nil)
	if err != nil {
		result.Error = err.Error()
		return result, err
	}

	req.Header.Set("Authorization", "Bearer "+account.AccessToken)
	req.Header.Set("anthropic-version", claudeAPIVersion)
	req.Header.Set("User-Agent", claudeUserAgent)
	req.Header.Set("Accept", "application/json")

	resp, err := f.client.Do(req)
	if err != nil {
		result.Error = err.Error()
		return result, err
	}
	defer resp.Body.Close()

	result.StatusCode = resp.StatusCode
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		result.Error = fmt.Sprintf("status %d: %s", resp.StatusCode, string(body))
		return result, fmt.Errorf("claude quota endpoint returned status %d", resp.StatusCode)
	}

	if err := parseClaudeUsageResponse(body, now, result); err != nil {
		result.Error = err.Error()
		return result, err
	}

	result.IsUsable = true
	return result, nil
}

func parseClaudeUsageResponse(raw []byte, now time.Time, out *AccountQuota) error {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		return fmt.Errorf("invalid claude quota json: %w", err)
	}

	// 1. 周配额 (seven_day)
	if rawWeekly, ok := doc["seven_day"]; ok && len(rawWeekly) > 0 {
		var cw claudeWindowRaw
		if err := json.Unmarshal(rawWeekly, &cw); err == nil && cw.Utilization != nil {
			var resetAt time.Time
			if cw.ResetsAt != nil {
				resetAt = *cw.ResetsAt
			}
			out.Weekly = &Window{
				WindowSeconds: 7 * 24 * 3600,
				UsedPercent:   *cw.Utilization,
				ResetAt:       resetAt,
			}
		}
	}

	// 2. 5小时窗口 (five_hour，如 Claude Pro/Team 启用)
	if raw5h, ok := doc["five_hour"]; ok && len(raw5h) > 0 {
		var cw claudeWindowRaw
		if err := json.Unmarshal(raw5h, &cw); err == nil && cw.Utilization != nil {
			var resetAt time.Time
			if cw.ResetsAt != nil {
				resetAt = *cw.ResetsAt
			}
			out.FiveHour = &Window{
				WindowSeconds: 5 * 3600,
				UsedPercent:   *cw.Utilization,
				ResetAt:       resetAt,
			}
		}
	}

	// 3. 模型子维度窗口 (如 seven_day_opus, seven_day_sonnet 等，若无独立 5h 则以 7d 兜底)
	for k, rawModel := range doc {
		if strings.HasPrefix(k, "seven_day_") && k != "seven_day" {
			var cw claudeWindowRaw
			if err := json.Unmarshal(rawModel, &cw); err == nil && cw.Utilization != nil {
				if out.Weekly == nil {
					var resetAt time.Time
					if cw.ResetsAt != nil {
						resetAt = *cw.ResetsAt
					}
					out.Weekly = &Window{
						WindowSeconds: 7 * 24 * 3600,
						UsedPercent:   *cw.Utilization,
						ResetAt:       resetAt,
					}
				}
			}
		}
	}

	return nil
}
