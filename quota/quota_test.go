package quota

import (
	"testing"
	"time"
)

func TestParseUsageResponseSuccess(t *testing.T) {
	now := time.Now()
	raw := []byte(`{
		"plan_type": "plus",
		"rate_limit": {
			"allowed": true,
			"limit_reached": false,
			"primary_window": {
				"limit_window_seconds": 18000,
				"used_percent": 12.5,
				"reset_after_seconds": 7200
			},
			"secondary_window": {
				"limit_window_seconds": 604800,
				"used_percent": 45.0,
				"reset_after_seconds": 259200
			}
		}
	}`)

	var out AccountQuota
	err := parseUsageResponse(raw, now, &out)
	if err != nil {
		t.Fatalf("parseUsageResponse error: %v", err)
	}

	if out.PlanType != "plus" {
		t.Errorf("expected plan_type 'plus', got '%s'", out.PlanType)
	}
	if out.FiveHour == nil {
		t.Fatalf("expected FiveHour window, got nil")
	}
	if out.FiveHour.UsedPercent != 12.5 {
		t.Errorf("expected 5h used 12.5, got %f", out.FiveHour.UsedPercent)
	}
	if out.Weekly == nil {
		t.Fatalf("expected Weekly window, got nil")
	}
	if out.Weekly.UsedPercent != 45.0 {
		t.Errorf("expected weekly used 45.0, got %f", out.Weekly.UsedPercent)
	}
}

func TestParseUsageResponseLimitReached(t *testing.T) {
	now := time.Now()
	raw := []byte(`{
		"plan_type": "team",
		"rate_limit": {
			"allowed": false,
			"limit_reached": true,
			"primary_window": {
				"limit_window_seconds": 18000,
				"used_percent": 80.0,
				"reset_after_seconds": 3600
			}
		}
	}`)

	var out AccountQuota
	err := parseUsageResponse(raw, now, &out)
	if err != nil {
		t.Fatalf("parseUsageResponse error: %v", err)
	}

	if out.FiveHour == nil {
		t.Fatalf("expected FiveHour window, got nil")
	}
	if out.FiveHour.UsedPercent != 100.0 {
		t.Errorf("expected limit_reached to force used_percent=100.0, got %f", out.FiveHour.UsedPercent)
	}
}

func TestParseClaudeUsageResponse(t *testing.T) {
	now := time.Now()
	raw := []byte(`{
		"seven_day": {
			"utilization": 35.5,
			"resets_at": "2026-10-30T12:00:00Z"
		},
		"five_hour": {
			"utilization": 15.0,
			"resets_at": "2026-10-25T16:00:00Z"
		}
	}`)

	var out AccountQuota
	err := parseClaudeUsageResponse(raw, now, &out)
	if err != nil {
		t.Fatalf("parseClaudeUsageResponse error: %v", err)
	}

	if out.Weekly == nil {
		t.Fatalf("expected Weekly window for claude, got nil")
	}
	if out.Weekly.UsedPercent != 35.5 {
		t.Errorf("expected claude weekly used 35.5, got %f", out.Weekly.UsedPercent)
	}
	if out.FiveHour == nil {
		t.Fatalf("expected FiveHour window for claude, got nil")
	}
	if out.FiveHour.UsedPercent != 15.0 {
		t.Errorf("expected claude 5h used 15.0, got %f", out.FiveHour.UsedPercent)
	}
}
