package scheduler

import (
	"fmt"
	"math"
	"sort"
	"time"
)

const (
	StrategyFillFirst      = "fill_first"
	StrategyResetFirst     = "reset_first"
	StrategyQuotaPressure  = "quota_pressure"
	StrategyBalanced       = "balanced"
	StrategyManualPriority = "manual_priority"

	DefaultPressureMinHours          = 0.5
	DefaultBalancedFiveHourSoftLimit = 85.0
	DefaultNewSessionMinRemaining    = 5.0
)

type Candidate struct {
	AccountID      string
	Provider       string
	PluginPriority int
	HostPriority   int
	Remaining      float64
	FiveHourUsed   float64
	ResetsAt       time.Time
	IsUsable       bool
	Disabled       bool
	CircuitOpen    bool
}

type RankedCandidate struct {
	AccountID string  `json:"auth_id"`
	Score     float64 `json:"score"`
	Group     int     `json:"group"`
	Reason    string  `json:"reason"`
	Remaining float64 `json:"remaining"`
}

type SkippedCandidate struct {
	AccountID string `json:"auth_id"`
	Reason    string `json:"reason"`
}

// RankCandidates 根据指定的调度策略对账号池进行评分与防震荡排序
func RankCandidates(
	candidates []Candidate,
	strategy string,
	now time.Time,
	currentActiveID string,
	minRemainingPercent float64,
) ([]RankedCandidate, []SkippedCandidate) {
	if minRemainingPercent <= 0 {
		minRemainingPercent = 2.0
	}

	var rankable []RankedCandidate
	var skipped []SkippedCandidate

	for _, c := range candidates {
		if c.Disabled {
			skipped = append(skipped, SkippedCandidate{
				AccountID: c.AccountID,
				Reason:    "disabled",
			})
			continue
		}
		if c.CircuitOpen {
			skipped = append(skipped, SkippedCandidate{
				AccountID: c.AccountID,
				Reason:    "circuit_open",
			})
			continue
		}
		if !c.IsUsable {
			skipped = append(skipped, SkippedCandidate{
				AccountID: c.AccountID,
				Reason:    "unusable_quota_data",
			})
			continue
		}

		// 容量检查：如果低于绝对阈值，直接跳过
		if c.Remaining <= minRemainingPercent {
			skipped = append(skipped, SkippedCandidate{
				AccountID: c.AccountID,
				Reason:    fmt.Sprintf("exhausted: %.1f%% remaining", c.Remaining),
			})
			continue
		}

		// NewSessionMinRemaining 检查：非当前主力账号若低于软阈值，防震荡不予晋升
		if c.AccountID != currentActiveID && c.Remaining < DefaultNewSessionMinRemaining {
			skipped = append(skipped, SkippedCandidate{
				AccountID: c.AccountID,
				Reason:    fmt.Sprintf("low_headroom: %.1f%% left (below %.0f%%)", c.Remaining, DefaultNewSessionMinRemaining),
			})
			continue
		}

		rc := RankedCandidate{
			AccountID: c.AccountID,
			Remaining: c.Remaining,
		}

		hours := math.Inf(1)
		resetDesc := "no reset reported"
		if !c.ResetsAt.IsZero() {
			diff := c.ResetsAt.Sub(now)
			hours = diff.Hours()
			if diff > 0 {
				resetDesc = fmt.Sprintf("resets in %s", formatDuration(diff))
			} else {
				resetDesc = "reset pending"
			}
		}

		switch strategy {
		case StrategyFillFirst:
			// FillFirst: 只要当前主力可用，赋予极高得分保持固定，用完才让位
			if c.AccountID == currentActiveID {
				rc.Score = 1000000.0
				rc.Reason = fmt.Sprintf("Primary active account (%.0f%% left)", c.Remaining)
			} else {
				rc.Score = c.Remaining
				rc.Reason = fmt.Sprintf("Reserve account (%.0f%% left, %s)", c.Remaining, resetDesc)
			}

		case StrategyQuotaPressure:
			h := hours
			if math.IsInf(h, 1) || h <= 0 {
				h = 168.0 // 默认按 7 天周窗口计算
			}
			rc.Score = c.Remaining / math.Max(h, DefaultPressureMinHours)
			rc.Reason = fmt.Sprintf("pressure %.2f (%.0f%% left, %s)", rc.Score, c.Remaining, resetDesc)

		case StrategyBalanced:
			rc.Score = -hours
			if c.FiveHourUsed >= DefaultBalancedFiveHourSoftLimit {
				rc.Group = 1
				rc.Reason = fmt.Sprintf("5h at %.0f%% (over %.0f%% soft limit); %s", c.FiveHourUsed, DefaultBalancedFiveHourSoftLimit, resetDesc)
			} else {
				rc.Group = 0
				rc.Reason = fmt.Sprintf("%s, %.0f%% left", resetDesc, c.Remaining)
			}

		case StrategyManualPriority:
			// 手动优先级：基础分采用 ResetFirst，排序时 PluginPriority 优先
			rc.Score = -hours
			rc.Reason = fmt.Sprintf("priority %d; %s, %.0f%% left", c.PluginPriority, resetDesc, c.Remaining)

		case StrategyResetFirst:
			fallthrough
		default:
			// ResetFirst: 重置倒计时越短，-hours 越大，排名越靠前
			rc.Score = -hours
			rc.Reason = fmt.Sprintf("%s, %.0f%% left", resetDesc, c.Remaining)
		}

		rankable = append(rankable, rc)
	}

	// 稳定排序逻辑：对齐 quota-router
	// 1. ManualPriority 下优先比较 PluginPriority
	// 2. Group 升序 (0 在 1 前)
	// 3. Score 降序 (得分高优先)
	// 4. Remaining 降序 (容量大优先)
	// 5. AccountID 字典序 (破除平分震荡)
	candMap := make(map[string]Candidate, len(candidates))
	for _, c := range candidates {
		candMap[c.AccountID] = c
	}

	sort.SliceStable(rankable, func(i, j int) bool {
		a, b := rankable[i], rankable[j]
		ca, cb := candMap[a.AccountID], candMap[b.AccountID]

		if strategy == StrategyManualPriority && ca.PluginPriority != cb.PluginPriority {
			return ca.PluginPriority > cb.PluginPriority
		}
		if a.Group != b.Group {
			return a.Group < b.Group
		}
		if a.Score != b.Score {
			return a.Score > b.Score
		}
		if a.Remaining != b.Remaining {
			return a.Remaining > b.Remaining
		}
		return a.AccountID < b.AccountID
	})

	return rankable, skipped
}

func formatDuration(d time.Duration) string {
	if d < 0 {
		return "0s"
	}
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	if h > 0 {
		return fmt.Sprintf("%dh%dm", h, m)
	}
	return fmt.Sprintf("%dm", m)
}
