package cpa

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"cuota-server/config"
)

type Client struct {
	cfg        config.CPAConfig
	httpClient *http.Client
	mu         sync.RWMutex
	accounts   map[string]*Account
}

func NewClient(cfg config.CPAConfig) *Client {
	return &Client{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		accounts: make(map[string]*Account),
	}
}

func (c *Client) LoadAccounts() ([]*Account, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var accounts []*Account

	if c.cfg.AuthDir != "" {
		if info, err := os.Stat(c.cfg.AuthDir); err == nil && info.IsDir() {
			entries, err := os.ReadDir(c.cfg.AuthDir)
			if err == nil {
				for _, entry := range entries {
					if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".json") {
						continue
					}
					fullPath := filepath.Join(c.cfg.AuthDir, entry.Name())
					data, err := os.ReadFile(fullPath)
					if err != nil {
						continue
					}
					acct, err := ParseAccountJSON(entry.Name(), data)
					if err != nil || (acct.Provider != "codex" && acct.Provider != "claude") || acct.AccessToken == "" {
						continue
					}
					if fileInfo, err := entry.Info(); err == nil && fileInfo != nil {
						acct.ModTime = fileInfo.ModTime()
					}
					accounts = append(accounts, acct)
				}
			}
		}
	}

	if len(accounts) == 0 && c.cfg.BaseURL != "" {
		apiAccounts, err := c.fetchAccountsFromAPI()
		if err == nil && len(apiAccounts) > 0 {
			accounts = apiAccounts
		}
	}

	c.accounts = make(map[string]*Account, len(accounts))
	for _, a := range accounts {
		c.accounts[a.ID] = a
	}

	return accounts, nil
}

func (c *Client) GetAccounts() []*Account {
	c.mu.RLock()
	defer c.mu.RUnlock()

	res := make([]*Account, 0, len(c.accounts))
	for _, a := range c.accounts {
		res = append(res, a)
	}
	return res
}

func (c *Client) GetAccount(id string) (*Account, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	acct, ok := c.accounts[id]
	return acct, ok
}

func (c *Client) UpdateAccountStatus(id string, disabled bool, priority int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	acct, ok := c.accounts[id]
	if !ok {
		return fmt.Errorf("account not found: %s", id)
	}

	acct.Disabled = disabled
	acct.Priority = priority

	if acct.RawJSON == nil {
		acct.RawJSON = make(map[string]any)
	}
	acct.RawJSON["disabled"] = disabled
	acct.RawJSON["priority"] = priority

	rawBytes, err := json.MarshalIndent(acct.RawJSON, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal updated account failed: %w", err)
	}

	if c.cfg.AuthDir != "" && acct.FileName != "" {
		fullPath := filepath.Join(c.cfg.AuthDir, acct.FileName)
		if err := os.WriteFile(fullPath, rawBytes, 0600); err != nil {
			log.Printf("[CPA] Warn: failed to write auth file %s: %v", fullPath, err)
		}
	}

	if c.cfg.BaseURL != "" {
		go c.syncAuthFileToAPI(acct.FileName, rawBytes)
	}

	return nil
}

func (c *Client) fetchAccountsFromAPI() ([]*Account, error) {
	url := strings.TrimRight(c.cfg.BaseURL, "/") + "/v0/management/auth-files"
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	c.setAuthHeader(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("cpa api error status: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var wrapper struct {
		Files []struct {
			Name     string `json:"name"`
			Provider string `json:"provider"`
			Disabled bool   `json:"disabled"`
			Priority int    `json:"priority"`
		} `json:"files"`
	}

	if err := json.Unmarshal(body, &wrapper); err != nil {
		return nil, err
	}

	var results []*Account
	for _, f := range wrapper.Files {
		if f.Provider != "codex" {
			continue
		}
		acct := &Account{
			ID:       f.Name,
			FileName: f.Name,
			Provider: f.Provider,
			Disabled: f.Disabled,
			Priority: f.Priority,
		}
		results = append(results, acct)
	}

	return results, nil
}

func (c *Client) syncAuthFileToAPI(filename string, data []byte) {
	if c.cfg.BaseURL == "" || filename == "" {
		return
	}
	url := strings.TrimRight(c.cfg.BaseURL, "/") + "/v0/management/auth-files"

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", filename)
	if err == nil {
		_, _ = part.Write(data)
		_ = writer.Close()

		req, err := http.NewRequest(http.MethodPost, url, &body)
		if err == nil {
			req.Header.Set("Content-Type", writer.FormDataContentType())
			c.setAuthHeader(req)
			resp, err := c.httpClient.Do(req)
			if err == nil {
				_ = resp.Body.Close()
			}
		}
	}
}

func (c *Client) setAuthHeader(req *http.Request) {
	if c.cfg.SecretKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.SecretKey)
		req.Header.Set("X-Management-Key", c.cfg.SecretKey)
	}
}
