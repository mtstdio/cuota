package config

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server    ServerConfig    `yaml:"server"`
	CPA       CPAConfig       `yaml:"cpa"`
	Quota     QuotaConfig     `yaml:"quota"`
	Probe     ProbeConfig     `yaml:"probe"`
	Scheduler SchedulerConfig `yaml:"scheduler"`
	Storage   StorageConfig   `yaml:"storage"`
}

type ServerConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	BasePath string `yaml:"base_path"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type CPAConfig struct {
	BaseURL   string `yaml:"base_url"`
	SecretKey string `yaml:"secret_key"`
	AuthDir   string `yaml:"auth_dir"`
}

type QuotaConfig struct {
	PollInterval        time.Duration `yaml:"poll_interval"`
	MinRemainingPercent float64       `yaml:"min_remaining_percent"`
	RequestTimeout      time.Duration `yaml:"request_timeout"`
}

type ProbeConfig struct {
	Enabled        bool          `yaml:"enabled"`
	Model          string        `yaml:"model"`
	Endpoint       string        `yaml:"endpoint"`
	Timeout        time.Duration `yaml:"timeout"`
	PostResetDelay time.Duration `yaml:"post_reset_delay"`
}

type SchedulerConfig struct {
	Strategy string `yaml:"strategy"`
}

type StorageConfig struct {
	StatePath string `yaml:"state_path"`
}

func DefaultConfig() Config {
	return Config{
		Server: ServerConfig{
			Host:     "0.0.0.0",
			Port:     8318,
			Username: "admin",
			Password: "admin",
		},
		CPA: CPAConfig{
			BaseURL:   "http://127.0.0.1:8317",
			SecretKey: "",
			AuthDir:   "~/.cli-proxy-api",
		},
		Quota: QuotaConfig{
			PollInterval:        60 * time.Second,
			MinRemainingPercent: 2.0,
			RequestTimeout:      15 * time.Second,
		},
		Probe: ProbeConfig{
			Enabled:        true,
			Model:          "gpt-5.6-luna",
			Endpoint:       "https://chatgpt.com/backend-api/codex/responses",
			Timeout:        30 * time.Second,
			PostResetDelay: 1 * time.Minute,
		},
		Scheduler: SchedulerConfig{
			Strategy: "fill_first",
		},
		Storage: StorageConfig{
			StatePath: "data/state.json",
		},
	}
}

func ExpandPath(path string) string {
	if strings.HasPrefix(path, "~/") || path == "~" {
		home, err := os.UserHomeDir()
		if err == nil {
			if path == "~" {
				return home
			}
			return filepath.Join(home, path[2:])
		}
	}
	return path
}

func CleanBasePath(p string) string {
	p = strings.TrimSpace(p)
	if p == "" || p == "/" {
		return ""
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return strings.TrimRight(p, "/")
}

func LoadConfig(path string) (*Config, error) {
	cfg := DefaultConfig()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			// Apply env vars even if config file doesn't exist
			applyEnvOverrides(&cfg)
			return &cfg, nil
		}
		return nil, err
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	cfg.CPA.AuthDir = ExpandPath(cfg.CPA.AuthDir)
	applyEnvOverrides(&cfg)
	return &cfg, nil
}

func applyEnvOverrides(cfg *Config) {
	for _, envKey := range []string{"BASE_PATH", "PATH_PREFIX", "CUOTA_BASE_PATH", "SERVER_BASE_PATH"} {
		if val := strings.TrimSpace(os.Getenv(envKey)); val != "" {
			cfg.Server.BasePath = val
			break
		}
	}
	cfg.Server.BasePath = CleanBasePath(cfg.Server.BasePath)

	for _, envKey := range []string{"STATE_PATH", "CUOTA_STATE_PATH"} {
		if val := strings.TrimSpace(os.Getenv(envKey)); val != "" {
			cfg.Storage.StatePath = val
			break
		}
	}
}
