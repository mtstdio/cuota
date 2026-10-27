package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cuota-server/config"
	"cuota-server/cpa"
	"cuota-server/probe"
	"cuota-server/quota"
	"cuota-server/scheduler"
)

func TestWebServerEndpoints(t *testing.T) {
	cfg := config.DefaultConfig()
	cpaClient := cpa.NewClient(cfg.CPA)
	fetcher := quota.NewFetcher(cfg.Quota.RequestTimeout)
	prober := probe.NewController(cfg.Probe)
	mgr := scheduler.NewManager(&cfg, cpaClient, fetcher, prober)
	srv := NewServer(cfg.Server, mgr)

	// 1. Test index.html served
	subFS, err := distFS.ReadFile("dist/index.html")
	if err != nil {
		t.Fatalf("dist/index.html not embedded properly: %v", err)
	}
	if !strings.Contains(string(subFS), "Cuota Dashboard") {
		t.Errorf("expected embedded index.html to contain title")
	}

	// 2. Test /api/overview
	reqOverview := httptest.NewRequest(http.MethodGet, "/api/overview", nil)
	wOverview := httptest.NewRecorder()
	srv.handleOverview(wOverview, reqOverview)
	if wOverview.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", wOverview.Code)
	}

	// 3. Test /api/status
	reqStatus := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	wStatus := httptest.NewRecorder()
	srv.handleStatus(wStatus, reqStatus)

	if wStatus.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", wStatus.Code)
	}

	var statusDoc map[string]any
	if err := json.Unmarshal(wStatus.Body.Bytes(), &statusDoc); err != nil {
		t.Fatalf("failed to decode /api/status response: %v", err)
	}

	if _, ok := statusDoc["accounts"]; !ok {
		t.Errorf("expected 'accounts' field in status response")
	}
	if _, ok := statusDoc["probe_logs"]; !ok {
		t.Errorf("expected 'probe_logs' field in status response")
	}
}

func TestAuthMiddleware(t *testing.T) {
	cfg := config.ServerConfig{
		Password: "secretpassword",
	}
	srv := NewServer(cfg, nil)

	handler := srv.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))

	// 1. 静态资源路由（如 / 或 /assets/index.js）无凭证直通放行 -> 200
	reqStatic := httptest.NewRequest(http.MethodGet, "/", nil)
	wStatic := httptest.NewRecorder()
	handler.ServeHTTP(wStatic, reqStatic)
	if wStatic.Code != http.StatusOK {
		t.Errorf("expected static request to bypass auth with 200, got %d", wStatic.Code)
	}

	// 2. API 请求无凭据 -> 401，且绝不包含 WWW-Authenticate 响应头
	reqAPI := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	wAPI := httptest.NewRecorder()
	handler.ServeHTTP(wAPI, reqAPI)
	if wAPI.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized for API without key, got %d", wAPI.Code)
	}
	if wAPI.Header().Get("WWW-Authenticate") != "" {
		t.Errorf("WWW-Authenticate header must NOT be set to prevent browser native dialogs, got %s", wAPI.Header().Get("WWW-Authenticate"))
	}

	// 3. API 请求携带错误凭据 -> 401
	reqWrong := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	reqWrong.Header.Set("Authorization", "Bearer wrongpassword")
	wWrong := httptest.NewRecorder()
	handler.ServeHTTP(wWrong, reqWrong)
	if wWrong.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for wrong token, got %d", wWrong.Code)
	}

	// 4. API 请求携带正确 Bearer Token -> 200
	reqValid := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	reqValid.Header.Set("Authorization", "Bearer secretpassword")
	wValid := httptest.NewRecorder()
	handler.ServeHTTP(wValid, reqValid)
	if wValid.Code != http.StatusOK {
		t.Errorf("expected 200 for valid Bearer token, got %d", wValid.Code)
	}

	// 5. API 请求携带 Basic Auth 格式凭据 -> 200
	reqBasic := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	reqBasic.SetBasicAuth("", "secretpassword")
	wBasic := httptest.NewRecorder()
	handler.ServeHTTP(wBasic, reqBasic)
	if wBasic.Code != http.StatusOK {
		t.Errorf("expected 200 for valid Basic auth password, got %d", wBasic.Code)
	}
}

func TestBasePathRouting(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Server.BasePath = "/cuota"
	cpaClient := cpa.NewClient(cfg.CPA)
	fetcher := quota.NewFetcher(cfg.Quota.RequestTimeout)
	prober := probe.NewController(cfg.Probe)
	mgr := scheduler.NewManager(&cfg, cpaClient, fetcher, prober)
	srv := NewServer(cfg.Server, mgr)

	// Mock Start handler setup
	mux := http.NewServeMux()
	mux.HandleFunc("/api/overview", srv.handleOverview)
	mux.HandleFunc(pluginAPIBase+"/overview", srv.handleOverview)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("index.html"))
	})

	basePath := config.CleanBasePath(srv.cfg.BasePath)
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
				r2.URL = r.URL
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

	// 1. /cuota should redirect to /cuota/ (307)
	req1 := httptest.NewRequest(http.MethodGet, "/cuota", nil)
	w1 := httptest.NewRecorder()
	rootHandler.ServeHTTP(w1, req1)
	if w1.Code != http.StatusTemporaryRedirect {
		t.Errorf("expected 307 redirect, got %d", w1.Code)
	}
	if loc := w1.Header().Get("Location"); loc != "/cuota/" {
		t.Errorf("expected Location /cuota/, got %s", loc)
	}

	// 2. /cuota/ should return 200
	req2 := httptest.NewRequest(http.MethodGet, "/cuota/", nil)
	w2 := httptest.NewRecorder()
	rootHandler.ServeHTTP(w2, req2)
	if w2.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w2.Code)
	}

	// 3. /cuota/api/overview should return 200
	req3 := httptest.NewRequest(http.MethodGet, "/cuota/api/overview", nil)
	w3 := httptest.NewRecorder()
	rootHandler.ServeHTTP(w3, req3)
	if w3.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w3.Code)
	}

	// 4. /cuota/v0/management/plugins/... should return 200
	req4 := httptest.NewRequest(http.MethodGet, "/cuota"+pluginAPIBase+"/overview", nil)
	w4 := httptest.NewRecorder()
	rootHandler.ServeHTTP(w4, req4)
	if w4.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w4.Code)
	}

	// 5. /api/overview without prefix should also return 200
	req5 := httptest.NewRequest(http.MethodGet, "/api/overview", nil)
	w5 := httptest.NewRecorder()
	rootHandler.ServeHTTP(w5, req5)
	if w5.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w5.Code)
	}
}

func TestConfigPatchAndSimulate(t *testing.T) {
	cfg := config.DefaultConfig()
	cpaClient := cpa.NewClient(cfg.CPA)
	fetcher := quota.NewFetcher(cfg.Quota.RequestTimeout)
	prober := probe.NewController(cfg.Probe)
	mgr := scheduler.NewManager(&cfg, cpaClient, fetcher, prober)
	srv := NewServer(cfg.Server, mgr)

	// 1. PATCH /api/config with strategy, account_alias, account_priority
	patchBody := `{"strategy":"reset_first","account_alias":{"test-id":"主力账号"},"account_priority":{"test-id":88}}`
	reqPatch := httptest.NewRequest(http.MethodPatch, "/api/config", strings.NewReader(patchBody))
	wPatch := httptest.NewRecorder()
	srv.handleConfig(wPatch, reqPatch)

	if wPatch.Code != http.StatusOK {
		t.Fatalf("expected 200 from handleConfig, got %d", wPatch.Code)
	}

	if mgr.Config().Scheduler.Strategy != "reset_first" {
		t.Errorf("expected strategy updated to reset_first, got %s", mgr.Config().Scheduler.Strategy)
	}
	labels := mgr.Labels()
	if labels["test-id"] != "主力账号" {
		t.Errorf("expected label to be 主力账号, got %s", labels["test-id"])
	}

	// 2. POST /api/simulate
	simBody := `{"provider":"codex","model":"gpt-5.6-luna","strategies":["reset_first","balanced"]}`
	reqSim := httptest.NewRequest(http.MethodPost, "/api/simulate", strings.NewReader(simBody))
	wSim := httptest.NewRecorder()
	srv.handleSimulate(wSim, reqSim)

	if wSim.Code != http.StatusOK {
		t.Fatalf("expected 200 from handleSimulate, got %d", wSim.Code)
	}

	var simResp map[string]any
	if err := json.Unmarshal(wSim.Body.Bytes(), &simResp); err != nil {
		t.Fatalf("failed to decode simulate response: %v", err)
	}
	if simResp["current_policy"] != "reset_first" {
		t.Errorf("expected current_policy reset_first, got %v", simResp["current_policy"])
	}
}
