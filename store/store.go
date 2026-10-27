package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// State 定义需要落盘持久化的数据结构
type State struct {
	Strategy             string            `json:"strategy"`
	AccountAlias         map[string]string `json:"account_alias,omitempty"`
	AccountAliases       map[string]string `json:"account_aliases,omitempty"`
	AccountPriority      map[string]int    `json:"account_priority"`
	Decisions            []any             `json:"decisions"`
	LastWarmupDate       string            `json:"last_warmup_date,omitempty"`
	Timezone             string            `json:"timezone,omitempty"`
	QuietHoursEnabled    *bool             `json:"quiet_hours_enabled,omitempty"`
	QuietHoursStart      string            `json:"quiet_hours_start,omitempty"`
	QuietHoursEnd        string            `json:"quiet_hours_end,omitempty"`
	MorningWarmupEnabled *bool             `json:"morning_warmup_enabled,omitempty"`
	MorningWarmupTime    string            `json:"morning_warmup_time,omitempty"`
	UpdatedAt            time.Time         `json:"updated_at"`
}

// Store 管理本地 state.json 的安全原子读写
type Store struct {
	path string
	mu   sync.RWMutex
}

// NewStore 创建持久化存储管理器
func NewStore(path string) (*Store, error) {
	if path == "" {
		path = "state.json"
	}
	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory for store: %w", err)
		}
	}
	return &Store{path: path}, nil
}

// Load 读取持久化数据。如果文件不存在则返回包含默认值的空白状态。
func (s *Store) Load() (*State, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return &State{
				Strategy:        "fill_first",
				AccountAliases:  make(map[string]string),
				AccountPriority: make(map[string]int),
				Decisions:       make([]any, 0),
				UpdatedAt:       time.Now(),
			}, nil
		}
		return nil, fmt.Errorf("failed to read state file: %w", err)
	}

	var st State
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("failed to parse state json: %w", err)
	}

	if st.AccountAliases == nil {
		if st.AccountAlias != nil {
			st.AccountAliases = st.AccountAlias
		} else {
			st.AccountAliases = make(map[string]string)
		}
	} else if st.AccountAlias != nil {
		for k, v := range st.AccountAlias {
			if _, exists := st.AccountAliases[k]; !exists {
				st.AccountAliases[k] = v
			}
		}
	}
	st.AccountAlias = st.AccountAliases

	if st.AccountPriority == nil {
		st.AccountPriority = make(map[string]int)
	}
	if st.Decisions == nil {
		st.Decisions = make([]any, 0)
	}
	return &st, nil
}

// Save 将状态原子持久化到磁盘
func (s *Store) Save(st *State) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	st.UpdatedAt = time.Now()
	if st.AccountAliases != nil && st.AccountAlias == nil {
		st.AccountAlias = st.AccountAliases
	} else if st.AccountAlias != nil && st.AccountAliases == nil {
		st.AccountAliases = st.AccountAlias
	}
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	tmpPath := s.path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open temp state file: %w", err)
	}

	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to write temp state file: %w", err)
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to sync temp state file: %w", err)
	}
	_ = f.Close()

	if err := os.Rename(tmpPath, s.path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to rename temp state file to %s: %w", s.path, err)
	}

	return nil
}
