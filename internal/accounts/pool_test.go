package accounts

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"autoclaw2api/internal/config"
	"autoclaw2api/internal/protocol"
	"autoclaw2api/internal/proxypool"
)

func TestParseAndMergeCredentialLossless(t *testing.T) {
	orig := `{
  "vendor": "autoclawcn",
  "region": "cn",
  "accessToken": "old-access-token",
  "refreshToken": "old-refresh-token",
  "expiresAt": 1700000000,
  "userId": "10086",
  "alias": "主力测试号",
  "note": "保留自定义备注",
  "customTag": "keep-me"
}`
	cred, err := ParseCredentialJSON([]byte(orig), "autoclawcn-10086.json")
	if err != nil {
		t.Fatalf("解析凭证失败: %v", err)
	}
	if cred.Region != protocol.RegionCN || cred.Alias != "主力测试号" || cred.UserID != "10086" {
		t.Fatalf("解析字段异常: %+v", cred)
	}

	merged, err := MergeTokensLossless([]byte(orig), "new-access-token", "new-refresh-token", 1800000000)
	if err != nil {
		t.Fatalf("无损合并失败: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(merged, &m); err != nil {
		t.Fatalf("解析合并后 JSON 失败: %v", err)
	}
	if m["accessToken"] != "new-access-token" || m["refreshToken"] != "new-refresh-token" {
		t.Fatalf("令牌未更新: %+v", m)
	}
	if m["alias"] != "主力测试号" || m["note"] != "保留自定义备注" || m["customTag"] != "keep-me" {
		t.Fatalf("无损合并丢失了自定义字段: %+v", m)
	}
}

func TestPoolRoundRobinCooldownAndFailover(t *testing.T) {
	dir := t.TempDir()
	authDir := filepath.Join(dir, "auths")
	dataDir := filepath.Join(dir, "data")
	cfgPath := filepath.Join(dir, "config.json")

	mgr, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	_, err = mgr.Update(func(c *config.Config) error {
		c.AuthDir = authDir
		c.DataDir = dataDir
		c.Retry.ShortCooldownSec = 30
		c.Retry.LongCooldownSec = 600
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// 写入 3 个国内账号和 1 个国际账号
	files := map[string]string{
		"autoclawcn-u1.json":     `{"region":"cn","accessToken":"tok-cn-1","userId":"u1"}`,
		"autoclawcn-u2.json":     `{"region":"cn","accessToken":"tok-cn-2","userId":"u2"}`,
		"autoclawcn-u3.json":     `{"region":"cn","accessToken":"tok-cn-3","userId":"u3"}`,
		"autoclawglobal-g1.json": `{"region":"global","accessToken":"tok-gl-1","userId":"g1"}`,
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(authDir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	pool, err := NewPool(mgr, nil)
	if err != nil {
		t.Fatalf("创建账号池失败: %v", err)
	}
	stats := pool.Stats()
	if stats.Total != 4 || stats.CNTotal != 3 || stats.GlobalTotal != 1 || stats.ReadyTotal != 4 {
		t.Fatalf("账号池统计不符: %+v", stats)
	}

	// 轮询验证
	a1, ok1 := pool.Acquire(protocol.RegionCN, nil)
	a2, ok2 := pool.Acquire(protocol.RegionCN, nil)
	a3, ok3 := pool.Acquire(protocol.RegionCN, nil)
	if !ok1 || !ok2 || !ok3 {
		t.Fatalf("轮询获取账号失败")
	}
	if a1.ID == a2.ID || a2.ID == a3.ID {
		t.Fatalf("轮询未在多个账号间分发: %s, %s, %s", a1.ID, a2.ID, a3.ID)
	}

	// 错误分级：u1 会话失效，u2 402 长冷却，u3 仍可正常切号命中
	pool.ReportError("autoclawcn-u1", protocol.ClassifyError(200, 410000, "not logged in", 0))
	pool.ReportError("autoclawcn-u2", protocol.ClassifyError(402, 0, "余额不足", 0))

	next, ok := pool.Acquire(protocol.RegionCN, nil)
	if !ok || next.ID != "autoclawcn-u3" {
		t.Fatalf("故障切号未正确避开失效与冷却账号: got=%v ok=%v", next, ok)
	}

	// u3 触发 429 短冷却后国内区应无可用账号
	pool.ReportError("autoclawcn-u3", protocol.ClassifyError(429, 0, "too many requests", 10*time.Second))
	if _, ok := pool.Acquire(protocol.RegionCN, nil); ok {
		t.Fatalf("全部国内账号不可用时不应继续调度")
	}
	// 国际区不受影响
	gl, ok := pool.Acquire(protocol.RegionGlobal, nil)
	if !ok || gl.ID != "autoclawglobal-g1" {
		t.Fatalf("国际区账号调度应保持独立可用")
	}
}

func TestRouterPrefixAndAliasLoadBalance(t *testing.T) {
	dir := t.TempDir()
	mgr, err := config.Load(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = mgr.Update(func(c *config.Config) error {
		c.AuthDir = filepath.Join(dir, "auths")
		c.DataDir = filepath.Join(dir, "data")
		c.Routing.Aliases = map[string][]string{
			"smart-pool": {"autoclawcn:glm-5.3-flash", "autoclawglobal:openclaw"},
		}
		c.Routing.DisabledModels = []string{"glm-2"}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	pool, err := NewPool(mgr, nil)
	if err != nil {
		t.Fatal(err)
	}
	proxyMgr, err := proxypool.NewManager(filepath.Join(dir, "data"), config.ProxyConfig{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	router := NewRouter(mgr, pool, proxyMgr, nil)

	// 禁用模型拦截
	if _, err := router.ResolveTargets("glm-2"); err == nil {
		t.Fatalf("禁用模型应当被拦截")
	}

	// 前缀定点路由
	targets, err := router.ResolveTargets("autoclawglobal:openclaw")
	if err != nil || len(targets) != 1 || targets[0].Region != protocol.RegionGlobal || targets[0].UpstreamModel != "openclaw" {
		t.Fatalf("前缀定点路由结果异常: %+v, err=%v", targets, err)
	}

	// 别名跨区轮询
	t1, err := router.ResolveTargets("smart-pool")
	if err != nil || len(t1) != 2 {
		t.Fatalf("别名路由解析失败: %+v, err=%v", t1, err)
	}
	t2, err := router.ResolveTargets("smart-pool")
	if err != nil || len(t2) != 2 {
		t.Fatalf("别名二次路由解析失败: %+v, err=%v", t2, err)
	}
	if t1[0].Region == t2[0].Region {
		t.Fatalf("同名别名未在多次调用间轮换优先目标区域: %+v vs %+v", t1[0], t2[0])
	}
}

func TestRouterRealtimeSyncAndEmptyWhenNoActiveAccount(t *testing.T) {
	var failUpstream atomic.Bool
	cnServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != protocol.PathModelsConfig {
			http.NotFound(w, r)
			return
		}
		if failUpstream.Load() {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("upstream down"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0,"msg":"ok","data":{"models":[{"id":"cn-live-model","name":"CN Live"}]}}`))
	}))
	defer cnServer.Close()

	dir := t.TempDir()
	authDir := filepath.Join(dir, "auths")
	dataDir := filepath.Join(dir, "data")
	mgr, err := config.Load(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = mgr.Update(func(c *config.Config) error {
		c.AuthDir = authDir
		c.DataDir = dataDir
		c.Upstream.CNBaseURL = cnServer.URL
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	pool, err := NewPool(mgr, nil)
	if err != nil {
		t.Fatal(err)
	}
	proxyMgr, err := proxypool.NewManager(dataDir, config.ProxyConfig{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	router := NewRouter(mgr, pool, proxyMgr, nil)

	// 1. 无账号时，双区均不得暴露任何硬编码静态模型
	if got := router.ListOpenAIModels(); len(got) != 0 {
		t.Fatalf("无有效账号时 /v1/models 应为空，实际返回 %d 个: %+v", len(got), got)
	}
	if got := router.ListAllModelsForAdmin(); len(got) != 0 {
		t.Fatalf("无有效账号时控制台模型列表应为空，实际返回 %d 个", len(got))
	}

	// 2. 添加一个国内账号后，实时向上游同步成功
	if _, err := pool.SaveCredential(&Credential{
		Region:      protocol.RegionCN,
		AccessToken: "tok-cn-live",
		UserID:      "u100",
	}, nil); err != nil {
		t.Fatal(err)
	}
	models, err := router.SyncRegionModels(context.Background(), protocol.RegionCN)
	if err != nil || len(models) != 1 || models[0].ID != "cn-live-model" {
		t.Fatalf("国内区实时模型同步失败: models=%+v err=%v", models, err)
	}
	if got := router.ListOpenAIModels(); len(got) != 2 { // both 模式：裸名 + autoclawcn: 前缀
		t.Fatalf("同步后应暴露 2 条记录，实际: %+v", got)
	}

	// 3. 上游探测失败时返回明确错误；但区域仍有活跃账号，
	//    必须沿用上一轮成功同步的缓存模型，不能因为一次瞬时抖动就清空 /v1/models
	failUpstream.Store(true)
	if _, err := router.SyncRegionModels(context.Background(), protocol.RegionCN); err == nil {
		t.Fatalf("上游探测失败时应返回错误")
	}
	if got := router.ListOpenAIModels(); len(got) != 2 {
		t.Fatalf("瞬时探测失败应保留上次缓存（2 条），实际: %+v", got)
	}
	st := router.SyncStatuses()["cn"]
	if st.ErrorMessage == "" {
		t.Fatalf("探测失败后应记录清晰失败原因供页面展示")
	}

	// 4. 恢复探测成功后，停用该最后一个账号，目录立即清空且不暴露
	failUpstream.Store(false)
	if _, err := router.SyncRegionModels(context.Background(), protocol.RegionCN); err != nil {
		t.Fatal(err)
	}
	disabled := true
	if err := pool.UpdateMeta("autoclawcn-u100", nil, nil, &disabled); err != nil {
		t.Fatal(err)
	}
	if got := router.ListOpenAIModels(); len(got) != 0 {
		t.Fatalf("最后一个账号停用后，该区模型目录应立即为空，实际: %+v", got)
	}
}
