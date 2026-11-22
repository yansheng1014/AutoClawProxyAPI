package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"autoclaw2api/internal/accounts"
	"autoclaw2api/internal/config"
	"autoclaw2api/internal/logging"
	"autoclaw2api/internal/protocol"
	"autoclaw2api/internal/proxypool"
	"autoclaw2api/internal/scheduler"
	"autoclaw2api/web"
)

type oauthSession struct {
	Provider    protocol.OAuthProvider
	DeviceID    string
	NavigateURI string
	ExpiresAt   time.Time
}

// Server 聚合多协议 AI 网关、代理池与终端极客风 Web 控制台。
type Server struct {
	cfgMgr    *config.Manager
	pool      *accounts.Pool
	router    *accounts.Router
	proxyMgr  *proxypool.Manager
	scheduler *scheduler.Engine
	logger    *logging.Logger
	startedAt time.Time

	oauthMu       sync.Mutex
	oauthSessions map[string]oauthSession

	sessionMu     sync.RWMutex
	panelSessions map[string]panelSession

	// sticky 维护「会话 -> 账号」的短时绑定，用于提升上游 Prompt Cache 命中率
	sticky *accounts.StickyBinder
}

// NewServer 构造网关服务实例。
func NewServer(
	cfgMgr *config.Manager,
	pool *accounts.Pool,
	router *accounts.Router,
	proxyMgr *proxypool.Manager,
	sched *scheduler.Engine,
	logger *logging.Logger,
) *Server {
	return &Server{
		cfgMgr:        cfgMgr,
		pool:          pool,
		router:        router,
		proxyMgr:      proxyMgr,
		scheduler:     sched,
		logger:        logger,
		startedAt:     time.Now(),
		oauthSessions: make(map[string]oauthSession),
		panelSessions: make(map[string]panelSession),
		sticky:        accounts.NewStickyBinder(),
	}
}

// Handler 返回挂载全部路由与中间件的根 HTTP 处理器。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// 多协议 AI 网关端点（仅受 API Key 鉴权保护，与面板密码完全隔离）
	mux.HandleFunc("GET /v1/models", s.withAPIAuth(s.handleListModels))
	mux.HandleFunc("POST /v1/chat/completions", s.withAPIAuth(s.handleChatCompletions))
	mux.HandleFunc("POST /v1/responses", s.withAPIAuth(s.handleResponses))
	mux.HandleFunc("POST /v1/messages", s.withAPIAuth(s.handleAnthropicMessages))
	mux.HandleFunc("GET /health", s.handleHealth)

	// 面板登录密码认证端点
	mux.HandleFunc("GET /api/admin/auth/status", s.handlePanelAuthStatus)
	mux.HandleFunc("POST /api/admin/auth/setup", s.handlePanelAuthSetup)
	mux.HandleFunc("POST /api/admin/auth/login", s.handlePanelAuthLogin)
	mux.HandleFunc("POST /api/admin/auth/logout", s.handlePanelAuthLogout)
	mux.HandleFunc("POST /api/admin/auth/password", s.withPanelAuth(s.handlePanelAuthChangePassword))

	// Web 管理控制台 API（受面板登录会话保护）
	mux.HandleFunc("GET /api/admin/overview", s.withPanelAuth(s.handleAdminOverview))

	// 账号池与登录
	mux.HandleFunc("GET /api/admin/accounts", s.withPanelAuth(s.handleAdminListAccounts))
	mux.HandleFunc("POST /api/admin/accounts/import", s.withPanelAuth(s.handleAdminImportAccount))
	mux.HandleFunc("POST /api/admin/accounts/sms/send", s.withPanelAuth(s.handleAdminSMSSend))
	mux.HandleFunc("POST /api/admin/accounts/sms/verify", s.withPanelAuth(s.handleAdminSMSVerify))
	mux.HandleFunc("GET /api/admin/accounts/oauth/captcha", s.withPanelAuth(s.handleAdminOAuthCaptcha))
	mux.HandleFunc("POST /api/admin/accounts/oauth/start", s.withPanelAuth(s.handleAdminOAuthStart))
	mux.HandleFunc("POST /api/admin/accounts/oauth/callback", s.withPanelAuth(s.handleAdminOAuthCallback))
	mux.HandleFunc("POST /api/admin/accounts/refresh-balances", s.withPanelAuth(s.handleAdminRefreshAllBalances))
	mux.HandleFunc("POST /api/admin/accounts/batch", s.withPanelAuth(s.handleAdminBatchAccounts))
	mux.HandleFunc("PUT /api/admin/accounts/{id}", s.withPanelAuth(s.handleAdminUpdateAccount))
	mux.HandleFunc("DELETE /api/admin/accounts/{id}", s.withPanelAuth(s.handleAdminDeleteAccount))
	mux.HandleFunc("POST /api/admin/accounts/{id}/refresh-token", s.withPanelAuth(s.handleAdminRefreshToken))
	mux.HandleFunc("POST /api/admin/accounts/{id}/refresh-balance", s.withPanelAuth(s.handleAdminRefreshBalance))
	mux.HandleFunc("PUT /api/admin/accounts/{id}/proxy", s.withPanelAuth(s.handleAdminAccountProxyBinding))

	// 独立 API Key 管理（一键随机生成 vKey-）
	mux.HandleFunc("GET /api/admin/apikeys", s.withPanelAuth(s.handleAdminListAPIKeys))
	mux.HandleFunc("POST /api/admin/apikeys", s.withPanelAuth(s.handleAdminCreateAPIKey))
	mux.HandleFunc("PUT /api/admin/apikeys/{id}", s.withPanelAuth(s.handleAdminUpdateAPIKey))
	mux.HandleFunc("DELETE /api/admin/apikeys/{id}", s.withPanelAuth(s.handleAdminDeleteAPIKey))

	// 代理池与三级出口绑定
	mux.HandleFunc("GET /api/admin/proxies", s.withPanelAuth(s.handleAdminProxiesSnapshot))
	mux.HandleFunc("POST /api/admin/proxies/import", s.withPanelAuth(s.handleAdminProxiesImport))
	mux.HandleFunc("POST /api/admin/proxies/probe-all", s.withPanelAuth(s.handleAdminProxyProbeAll))
	mux.HandleFunc("PUT /api/admin/proxies/health", s.withPanelAuth(s.handleAdminProxyHealthConfig))
	mux.HandleFunc("PUT /api/admin/proxies/regions/{region}", s.withPanelAuth(s.handleAdminProxyRegionBinding))
	mux.HandleFunc("PUT /api/admin/proxies/{id}", s.withPanelAuth(s.handleAdminProxyUpdate))
	mux.HandleFunc("DELETE /api/admin/proxies/{id}", s.withPanelAuth(s.handleAdminProxyDelete))
	mux.HandleFunc("POST /api/admin/proxies/{id}/probe", s.withPanelAuth(s.handleAdminProxyProbeOne))

	// 签到与任务
	mux.HandleFunc("POST /api/admin/checkin/run", s.withPanelAuth(s.handleAdminCheckinRun))
	mux.HandleFunc("GET /api/admin/checkin/history", s.withPanelAuth(s.handleAdminCheckinHistory))

	// 模型路由
	mux.HandleFunc("GET /api/admin/models", s.withPanelAuth(s.handleAdminModels))
	mux.HandleFunc("POST /api/admin/models/refresh", s.withPanelAuth(s.handleAdminRefreshModels))
	mux.HandleFunc("PUT /api/admin/models/routing", s.withPanelAuth(s.handleAdminUpdateRouting))

	// 系统设置与日志
	mux.HandleFunc("GET /api/admin/settings", s.withPanelAuth(s.handleAdminGetSettings))
	mux.HandleFunc("PUT /api/admin/settings", s.withPanelAuth(s.handleAdminUpdateSettings))
	mux.HandleFunc("GET /api/admin/logs", s.withPanelAuth(s.handleAdminLogs))
	mux.HandleFunc("DELETE /api/admin/logs", s.withPanelAuth(s.handleAdminClearLogs))

	// 内嵌单页 Web 控制台
	mux.HandleFunc("GET /", s.handleConsoleIndex)

	return s.withRecovery(s.withCORS(mux))
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"service": "AutoClaw2API",
		"author":  "VILK",
		"version": config.Version,
	})
}

func (s *Server) handleConsoleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "/index.html" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(web.IndexHTML)
}

func (s *Server) withRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				if s.logger != nil {
					s.logger.Error("Gateway", "捕获未处理异常 (%s %s): %v\n%s", r.Method, r.URL.Path, rec, string(debug.Stack()))
				}
				writeJSON(w, http.StatusInternalServerError, map[string]any{
					"error": map[string]any{
						"message": fmt.Sprintf("网关内部异常: %v", rec),
						"type":    "server_error",
					},
				})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Api-Key, x-api-key, X-Panel-Session, anthropic-version, anthropic-beta")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// withAPIAuth 校验网关 API Key，同时兼容 Authorization: Bearer <key> 与 x-api-key: <key>。
func (s *Server) withAPIAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cfg := s.cfgMgr.Get()
		provided := extractClientKey(r)
		authEnabled, valid := cfg.ValidateClientAPIKey(provided)
		if !authEnabled || valid {
			next(w, r)
			return
		}
		if provided == "" {
			writeAuthError(w, r.URL.Path, "缺少网关访问密钥（请通过 Authorization: Bearer 或 x-api-key 传入）")
			return
		}
		writeAuthError(w, r.URL.Path, "无效或已停用的网关访问 API Key")
	}
}

func extractClientKey(r *http.Request) string {
	if v := strings.TrimSpace(r.Header.Get("x-api-key")); v != "" {
		return v
	}
	if v := strings.TrimSpace(r.Header.Get("X-Api-Key")); v != "" {
		return v
	}
	return extractBearerToken(r.Header.Get("Authorization"))
}

func extractBearerToken(header string) string {
	h := strings.TrimSpace(header)
	if len(h) > 7 && strings.EqualFold(h[:7], "Bearer ") {
		return strings.TrimSpace(h[7:])
	}
	return ""
}

func writeAuthError(w http.ResponseWriter, path, message string) {
	if strings.HasPrefix(path, "/v1/messages") {
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"type": "error",
			"error": map[string]any{
				"type":    "authentication_error",
				"message": message,
			},
		})
		return
	}
	writeJSON(w, http.StatusUnauthorized, map[string]any{
		"error": map[string]any{
			"message": message,
			"type":    "invalid_request_error",
			"code":    "invalid_api_key",
		},
	})
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func (s *Server) baseURLFor(region protocol.Region) string {
	cfg := s.cfgMgr.Get()
	if region.IsGlobal() {
		return protocol.ResolveBaseURL(region, cfg.Upstream.GlobalBaseURL)
	}
	return protocol.ResolveBaseURL(region, cfg.Upstream.CNBaseURL)
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func calcBackoff(attempt int, cfg config.RetryConfig, retryAfter time.Duration) time.Duration {
	if retryAfter > 0 {
		maxD := time.Duration(cfg.MaxBackoffMs) * time.Millisecond
		if retryAfter > maxD {
			return maxD
		}
		return retryAfter
	}
	base := time.Duration(cfg.BaseBackoffMs) * time.Millisecond
	maxD := time.Duration(cfg.MaxBackoffMs) * time.Millisecond
	d := base << min(attempt, 6)
	if d > maxD {
		return maxD
	}
	return d
}
