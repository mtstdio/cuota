package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStoreLoadSave(t *testing.T) {
	tmpDir := t.TempDir()
	statePath := filepath.Join(tmpDir, "state.json")

	s, err := NewStore(statePath)
	if err != nil {
		t.Fatalf("unexpected NewStore error: %v", err)
	}

	// 1. 初次读取，文件不存在，应该返回默认值
	st, err := s.Load()
	if err != nil {
		t.Fatalf("unexpected Load error: %v", err)
	}
	if st.Strategy != "fill_first" {
		t.Errorf("expected default strategy fill_first, got %s", st.Strategy)
	}
	if len(st.AccountAliases) != 0 {
		t.Errorf("expected empty aliases, got %d", len(st.AccountAliases))
	}

	// 2. 修改并保存
	st.Strategy = "reset_first"
	st.AccountAliases["acc1"] = "主力号A"
	st.AccountPriority["acc1"] = 50
	st.Decisions = append(st.Decisions, map[string]any{"id": 1, "action": "promote"})

	if err := s.Save(st); err != nil {
		t.Fatalf("unexpected Save error: %v", err)
	}

	// 3. 再次读取验证
	st2, err := s.Load()
	if err != nil {
		t.Fatalf("unexpected Load error after save: %v", err)
	}
	if st2.Strategy != "reset_first" {
		t.Errorf("expected strategy reset_first, got %s", st2.Strategy)
	}
	if st2.AccountAliases["acc1"] != "主力号A" {
		t.Errorf("expected alias 主力号A, got %s", st2.AccountAliases["acc1"])
	}
	if st2.AccountPriority["acc1"] != 50 {
		t.Errorf("expected priority 50, got %d", st2.AccountPriority["acc1"])
	}
	if len(st2.Decisions) != 1 {
		t.Errorf("expected 1 decision, got %d", len(st2.Decisions))
	}

	// 验证时间戳已更新
	if st2.UpdatedAt.IsZero() || time.Since(st2.UpdatedAt) > 5*time.Second {
		t.Errorf("expected valid UpdatedAt, got %v", st2.UpdatedAt)
	}

	// 确保临时文件已清理
	if _, err := os.Stat(statePath + ".tmp"); !os.IsNotExist(err) {
		t.Errorf("expected temp file to be removed")
	}
}
