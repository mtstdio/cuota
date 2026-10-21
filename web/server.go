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
	"time"

	"cuota-server/config"
	"cuota-server/scheduler"
)

//go:embed static/*
var staticFS embed.FS

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
		user, pass, ok := r.BasicAuth()
		if !ok || subtle.ConstantTimeCompare([]byte(user), []byte(s.cfg.Username)) != 1 ||
			subtle.ConstantTimeCompare([]byte(pass), []byte(s.cfg.Password)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="Cuota Management"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()

	// 嵌入静态文件服务
	subFS, err := fs.Sub(staticFS, "static")
	if err != nil {
		return fmt.Errorf("failed to load embedded static fs: %w", err)
	}
	fileServer := http.FileServer(http.FS(subFS))
	mux.Handle("/", fileServer)

	// API 路由
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/probe", s.handleProbe)
	mux.HandleFunc("/api/switch", s.handleSwitch)
	mux.HandleFunc("/api/account/toggle", s.handleToggle)
	mux.HandleFunc("/api/refresh", s.handleRefresh)

	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	s.srv = &http.Server{
		Addr:         addr,
		Handler:      s.basicAuthMiddleware(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.srv.Shutdown(shutdownCtx)
	}()

	log.Printf("[Web] Management Dashboard and API listening on http://%s", addr)
	if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.AccountID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid account_id"})
		return
	}

	ev, err := s.scheduler.ManualProbe(r.Context(), req.AccountID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"error":   err.Error(),
			"event":   ev,
			"message": "探测失败: " + err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"event":   ev,
		"message": "探测激活已执行完成",
	})
}

func (s *Server) handleSwitch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		AccountID string `json:"account_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.AccountID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid account_id"})
		return
	}

	if err := s.scheduler.ManualSwitch(req.AccountID); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"message": fmt.Sprintf("已成功将主力账号切换为 %s", req.AccountID),
	})
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.AccountID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid payload"})
		return
	}

	if err := s.scheduler.ToggleAccount(req.AccountID, req.Disabled); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	statusDesc := "已启用"
	if req.Disabled {
		statusDesc = "已停用"
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"message": fmt.Sprintf("账号 %s %s", req.AccountID, statusDesc),
	})
}

func (s *Server) handleRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.scheduler.TriggerRefresh()
	writeJSON(w, http.StatusOK, map[string]string{"message": "刷新指令已触发"})
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
