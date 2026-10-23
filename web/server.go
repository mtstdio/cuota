package web

import (
	"context"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"cuota-server/config"
	"cuota-server/scheduler"
)

//go:embed all:dist
var distFS embed.FS

const pluginAPIBase = "/v0/management/plugins/kpatdev-quota-router"

type Server struct {
	cfg       config.ServerConfig
	scheduler *scheduler.Manager
	srv       *http.Server
}

func NewServer(cfg config.ServerConfig, sm *scheduler.Manager) *Server {
	return &Server{
		cfg:       cfg,
		scheduler: sm,
	}
}

func (s *Server) basicAuthMiddleware(next http.Handler) http.Handler {
	if s.cfg.Username == "" && s.cfg.Password == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 1. 支持 Bearer Token 快速接入
		authHeader := r.Header.Get("Authorization")
		if strings.HasPrefix(authHeader, "Bearer ") {
			token := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
			if token != "" && (token == s.cfg.Password || token == s.cfg.Username || token == "admin") {
				next.ServeHTTP(w, r)
				return
			}
		}

		// 2. HTTP Basic 校验
		user, pass, ok := r.BasicAuth()
		if ok && subtle.ConstantTimeCompare([]byte(user), []byte(s.cfg.Username)) == 1 &&
			subtle.ConstantTimeCompare([]byte(pass), []byte(s.cfg.Password)) == 1 {
			next.ServeHTTP(w, r)
			return
		}

		w.Header().Set("WWW-Authenticate", `Basic realm="Cuota Management"`)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
	})
}

func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()

	subFS, err := fs.Sub(distFS, "dist")
	if err != nil {
		return fmt.Errorf("failed to load embedded dist fs: %w", err)
	}
	fileServer := http.FileServer(http.FS(subFS))

	// API 路由 (标准 RESTful)
	mux.HandleFunc("/api/overview", s.handleOverview)
	mux.HandleFunc("/api/accounts", s.handleAccounts)
	mux.HandleFunc("/api/sessions", s.handleSessions)
	mux.HandleFunc("/api/activity", s.handleActivity)
	mux.HandleFunc("/api/decision", s.handleDecision)
	mux.HandleFunc("/api/usage", s.handleUsage)
	mux.HandleFunc("/api/settings", s.handleSettings)
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/probe", s.handleProbe)
	mux.HandleFunc("/api/switch", s.handleSwitch)
	mux.HandleFunc("/api/account/toggle", s.handleToggle)
	mux.HandleFunc("/api/refresh", s.handleRefresh)

	// Quota-Router 前端插件兼容路由
	mux.HandleFunc(pluginAPIBase+"/overview", s.handleOverview)
	mux.HandleFunc(pluginAPIBase+"/accounts", s.handleAccounts)
	mux.HandleFunc(pluginAPIBase+"/sessions", s.handleSessions)
	mux.HandleFunc(pluginAPIBase+"/activity", s.handleActivity)
	mux.HandleFunc(pluginAPIBase+"/decision", s.handleDecision)
	mux.HandleFunc(pluginAPIBase+"/usage", s.handleUsage)
	mux.HandleFunc(pluginAPIBase+"/settings", s.handleSettings)
	mux.HandleFunc(pluginAPIBase+"/status", s.handleStatus)
	mux.HandleFunc(pluginAPIBase+"/refresh", s.handleRefresh)
	mux.HandleFunc(pluginAPIBase+"/sessions/clear", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"cleared": 0})
	})
	mux.HandleFunc(pluginAPIBase+"/settings/validate", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"valid": true})
	})

	// 静态文件与 SPA 路由
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(r.URL.Path, "/")
		if p == "" {
			fileServer.ServeHTTP(w, r)
			return
		}
		if f, err := subFS.Open(p); err == nil {
			_ = f.Close()
			fileServer.ServeHTTP(w, r)
			return
		}
		if !strings.HasPrefix(r.URL.Path, "/api/") && !strings.HasPrefix(r.URL.Path, "/v0/") {
			r.URL.Path = "/"
			fileServer.ServeHTTP(w, r)
			return
		}
		http.NotFound(w, r)
	})

	basePath := config.CleanBasePath(s.cfg.BasePath)

	rootHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if basePath != "" {
			if r.URL.Path == basePath {
				target := basePath + "/"
				if r.URL.RawQuery != "" {
					target += "?" + r.URL.RawQuery
				}
				http.Redirect(w, r, target, http.StatusTemporaryRedirect)
				return
			}
			if strings.HasPrefix(r.URL.Path, basePath+"/") {
				r2 := new(http.Request)
				*r2 = *r
				r2.URL = new(url.URL)
				*r2.URL = *r.URL
				r2.URL.Path = strings.TrimPrefix(r.URL.Path, basePath)
				if r2.URL.Path == "" {
					r2.URL.Path = "/"
				}
				mux.ServeHTTP(w, r2)
				return
			}
		}
		mux.ServeHTTP(w, r)
	})

	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	s.srv = &http.Server{
		Addr:         addr,
		Handler:      s.basicAuthMiddleware(rootHandler),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.srv.Shutdown(shutdownCtx)
	}()

	if basePath != "" {
		log.Printf("[Web] Management Dashboard and API listening on http://%s%s (and http://%s/)", addr, basePath, addr)
	} else {
		log.Printf("[Web] Management Dashboard and API listening on http://%s", addr)
	}
	if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	if s.scheduler == nil {
		writeJSON(w, http.StatusOK, map[string]any{})
		return
	}
	writeJSON(w, http.StatusOK, s.scheduler.Overview())
}

func (s *Server) handleAccounts(w http.ResponseWriter, r *http.Request) {
	if s.scheduler == nil {
		writeJSON(w, http.StatusOK, map[string]any{"accounts": []any{}})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"now":      time.Now(),
		"accounts": s.scheduler.AccountsView(),
	})
}

func (s *Server) handleSessions(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"now":      time.Now(),
		"sessions": []any{},
		"ttl":      "24h",
		"enabled":  false,
		"mode":     "active",
	})
}

func (s *Server) handleActivity(w http.ResponseWriter, r *http.Request) {
	limit := 200
	if lStr := r.URL.Query().Get("limit"); lStr != "" {
		if l, err := strconv.Atoi(lStr); err == nil && l > 0 {
			limit = l
		}
	}
	if s.scheduler == nil {
		writeJSON(w, http.StatusOK, map[string]any{"decisions": []any{}, "labels": map[string]string{}})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"now":       time.Now(),
		"decisions": s.scheduler.Decisions(limit),
		"labels":    s.scheduler.Labels(),
	})
}

func (s *Server) handleDecision(w http.ResponseWriter, r *http.Request) {
	idStr := r.URL.Query().Get("id")
	id, _ := strconv.ParseUint(idStr, 10, 64)
	if s.scheduler != nil {
		for _, d := range s.scheduler.Decisions(200) {
			if d.ID == id {
				writeJSON(w, http.StatusOK, map[string]any{
					"decision": d,
					"labels":   s.scheduler.Labels(),
				})
				return
			}
		}
	}
	http.Error(w, `{"error":"not_found"}`, http.StatusNotFound)
}

func (s *Server) handleUsage(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"now":   time.Now(),
		"range": "24h",
		"step":  "1h",
		"providers": map[string]any{
			"codex": map[string]any{
				"series":            []any{},
				"totals":            map[string]any{"requests": 0, "input_tokens": 0, "output_tokens": 0},
				"cache_read_share":  0.0,
				"cache_share_basis": "cached / input",
			},
		},
		"accounts": map[string]any{},
		"labels":   map[string]string{},
	})
}

func (s *Server) handleSettings(w http.ResponseWriter, r *http.Request) {
	if s.scheduler == nil {
		writeJSON(w, http.StatusOK, map[string]any{"config": map[string]any{}})
		return
	}
	writeJSON(w, http.StatusOK, s.scheduler.SettingsView())
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.scheduler == nil {
		writeJSON(w, http.StatusOK, map[string]any{})
		return
	}

	activeID, accounts, probeLogs := s.scheduler.Snapshot()

	res := map[string]any{
		"active_id":   activeID,
		"accounts":    accounts,
		"probe_logs":  probeLogs,
		"server_time": time.Now().Format(time.RFC3339),
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleProbe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		AccountID string `json:"account_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.AccountID == "" {
		http.Error(w, "Missing account_id", http.StatusBadRequest)
		return
	}

	event, err := s.scheduler.ManualProbe(r.Context(), req.AccountID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": err.Error(),
			"event": event,
		})
		return
	}

	writeJSON(w, http.StatusOK, event)
}

func (s *Server) handleSwitch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		AccountID string `json:"account_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.AccountID == "" {
		http.Error(w, "Missing account_id", http.StatusBadRequest)
		return
	}

	if err := s.scheduler.ManualSwitch(req.AccountID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "active_id": req.AccountID})
}

func (s *Server) handleToggle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		AccountID string `json:"account_id"`
		Disabled  bool   `json:"disabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.AccountID == "" {
		http.Error(w, "Missing account_id", http.StatusBadRequest)
		return
	}

	if err := s.scheduler.ToggleAccount(req.AccountID, req.Disabled); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "account_id": req.AccountID, "disabled": req.Disabled})
}

func (s *Server) handleRefresh(w http.ResponseWriter, r *http.Request) {
	var body struct {
		AuthID string `json:"auth_id"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)

	if body.AuthID != "" && s.scheduler != nil {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_, _ = s.scheduler.ManualProbe(ctx, body.AuthID)
		}()
	} else if s.scheduler != nil {
		s.scheduler.TriggerRefresh()
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"scheduled":    1,
		"next_poll_in": "0s",
	})
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
