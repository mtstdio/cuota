package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfigDefault(t *testing.T) {
	cfg, err := LoadConfig("non_existent_file.yaml")
	if err != nil {
		t.Fatalf("unexpected error loading non existent config: %v", err)
	}

	if cfg.Server.Port != 8318 {
		t.Errorf("expected default port 8318, got %d", cfg.Server.Port)
	}
	if cfg.Server.Username != "admin" || cfg.Server.Password != "admin" {
		t.Errorf("expected default auth admin/admin, got %s/%s", cfg.Server.Username, cfg.Server.Password)
	}
	if cfg.CPA.BaseURL != "http://127.0.0.1:8317" {
		t.Errorf("expected default base_url http://127.0.0.1:8317, got %s", cfg.CPA.BaseURL)
	}
	if !cfg.Probe.Enabled {
		t.Errorf("expected probe enabled by default")
	}
	if cfg.Probe.Model != "gpt-5.6-luna" {
		t.Errorf("expected default model gpt-5.6-luna, got %s", cfg.Probe.Model)
	}
}

func TestLoadConfigCustom(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")

	content := `
server:
  host: "127.0.0.1"
  port: 9000
  username: "myuser"
  password: "mypassword"
cpa:
  base_url: "http://localhost:8317"
  secret_key: "test-secret"
  auth_dir: "/tmp/auth"
quota:
  poll_interval: 30s
  min_remaining_percent: 5.0
probe:
  enabled: true
  model: "gpt-5-mini"
scheduler:
  strategy: "fill_first"
`
	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		t.Fatalf("write config file error: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("failed to load custom config: %v", err)
	}

	if cfg.Server.Port != 9000 {
		t.Errorf("expected port 9000, got %d", cfg.Server.Port)
	}
	if cfg.Server.Username != "myuser" || cfg.Server.Password != "mypassword" {
		t.Errorf("expected custom auth myuser/mypassword, got %s/%s", cfg.Server.Username, cfg.Server.Password)
	}
	if cfg.CPA.SecretKey != "test-secret" {
		t.Errorf("expected secret-key 'test-secret', got '%s'", cfg.CPA.SecretKey)
	}
	if cfg.Quota.PollInterval != 30*time.Second {
		t.Errorf("expected 30s poll interval, got %v", cfg.Quota.PollInterval)
	}
	if cfg.Probe.Model != "gpt-5-mini" {
		t.Errorf("expected model gpt-5-mini, got %s", cfg.Probe.Model)
	}
}

func TestBasePathEnvAndClean(t *testing.T) {
	if CleanBasePath("") != "" {
		t.Errorf("expected empty string")
	}
	if CleanBasePath("/") != "" {
		t.Errorf("expected empty string")
	}
	if CleanBasePath("/cuota") != "/cuota" {
		t.Errorf("expected /cuota, got %s", CleanBasePath("/cuota"))
	}
	if CleanBasePath("/cuota/") != "/cuota" {
		t.Errorf("expected /cuota, got %s", CleanBasePath("/cuota/"))
	}
	if CleanBasePath("cuota") != "/cuota" {
		t.Errorf("expected /cuota, got %s", CleanBasePath("cuota"))
	}

	t.Setenv("BASE_PATH", "/my-cuota/")
	cfg, err := LoadConfig("non_existent.yaml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Server.BasePath != "/my-cuota" {
		t.Errorf("expected /my-cuota, got %s", cfg.Server.BasePath)
	}
}
