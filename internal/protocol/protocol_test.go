package protocol

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestCalcAuthSign(t *testing.T) {
	ts := "1700000000"
	got := CalcAuthSign(ts)
	sum := md5.Sum([]byte("100003&1700000000&38d2391985e2369a5fb8227d8e6cd5e5"))
	want := hex.EncodeToString(sum[:])
	if got != want {
		t.Fatalf("签名计算不一致: got=%s want=%s", got, want)
	}
}

func TestThreeHeaderSetsIsolated(t *testing.T) {
	reqChat, _ := http.NewRequest(http.MethodPost, "https://example.com/chat", nil)
	AttachChatHeaders(reqChat, "tok-123", "glm-5.3-flash")
	if reqChat.Header.Get("X-Tm") != "mac" {
		t.Fatalf("对话头 X-Tm 应为 mac，实际: %s", reqChat.Header.Get("X-Tm"))
	}
	if reqChat.Header.Get("Authorization") != "Bearer tok-123" || reqChat.Header.Get("X-Authorization") != "Bearer tok-123" {
		t.Fatalf("对话头缺少双通道 Bearer")
	}
	if reqChat.Header.Get("X-Request-Model") != "glm-5.3-flash" {
		t.Fatalf("对话头缺少 X-Request-Model")
	}
	// 风控要求：trace 头不得暴露第三方工具名，且两个 trace 头应为同一个随机值
	traceUpper := reqChat.Header.Get("X-Trace-Id")
	traceLower := reqChat.Header.Get("x_trace_id")
	if traceUpper == "" || traceUpper != traceLower {
		t.Fatalf("两个 trace 头应为同一随机值，实际 X-Trace-Id=%q x_trace_id=%q", traceUpper, traceLower)
	}
	if len(traceUpper) != 16 {
		t.Fatalf("trace 头应为 16 位十六进制，实际 %q", traceUpper)
	}
	if strings.Contains(strings.ToLower(traceUpper), "autoclaw") {
		t.Fatalf("trace 头不得包含项目名，实际 %q", traceUpper)
	}
	if reqChat.Header.Get("X-Auth-Sign") != "" {
		t.Fatalf("对话头不应混入 UserAPI 签名头")
	}

	reqUser, _ := http.NewRequest(http.MethodPost, "https://example.com/user", nil)
	AttachUserHeaders(reqUser, "tok-123")
	if reqUser.Header.Get("X-Auth-Appid") != "100003" || reqUser.Header.Get("X-Auth-Sign") == "" {
		t.Fatalf("UserAPI 头缺少签名字段")
	}
	if reqUser.Header.Get("X-Harness-Type") != "zcode" || reqUser.Header.Get("User-Agent") != "AutoClaw/1.18.5" {
		t.Fatalf("UserAPI 头缺少客户端指纹或 UA")
	}

	reqOAuth, _ := http.NewRequest(http.MethodPost, "https://example.com/oauth", nil)
	AttachOAuthHeaders(reqOAuth, "")
	if reqOAuth.Header.Get("X-Tm") != "linux" {
		t.Fatalf("国际版 OAuth 头 X-Tm 必须固定为 linux，实际: %s", reqOAuth.Header.Get("X-Tm"))
	}
	if reqOAuth.Header.Get("X-Auth-Sign") == "" {
		t.Fatalf("国际版 OAuth 头缺少签名")
	}
}

func TestPrepareOutboundChatBodyAnti406(t *testing.T) {
	input := `{
		"model": "autoclawcn:glm-5.3-flash",
		"user": "someone",
		"max_tokens": 4096,
		"max_completion_tokens": 2048,
		"messages": [
			{"role": "system", "content": "You are Claude Code. Help the user."},
			{"role": "developer", "content": [{"type":"text","text":"You are a coding agent running in the Codex CLI tool."}]},
			{"role": "user", "content": "你好"}
		]
	}`
	outBytes := PrepareOutboundChatBody([]byte(input), "glm-5.3-flash")
	var out map[string]any
	if err := json.Unmarshal(outBytes, &out); err != nil {
		t.Fatalf("反序列化规整结果失败: %v", err)
	}
	if _, exists := out["user"]; exists {
		t.Fatalf("出站请求未删除 user 字段")
	}
	if out["model"] != "glm-5.3-flash" {
		t.Fatalf("model 未重写: %v", out["model"])
	}
	if out["max_tokens"].(float64) != 10000 {
		t.Fatalf("小正整数 max_tokens 应抬升至 10000，实际: %v", out["max_tokens"])
	}
	if out["max_completion_tokens"].(float64) != 2048 {
		t.Fatalf("max_completion_tokens 不应被修改，实际: %v", out["max_completion_tokens"])
	}

	msgs := out["messages"].([]any)
	sysContent := msgs[0].(map[string]any)["content"].(string)
	if !strings.HasPrefix(sysContent, SystemIdentityPrefix) {
		t.Fatalf("首条 system 未前置白名单前缀: %s", sysContent)
	}
	if strings.Contains(strings.ToLower(sysContent), "claude code") {
		t.Fatalf("外来身份句未替换: %s", sysContent)
	}
	devParts := msgs[1].(map[string]any)["content"].([]any)
	devText := devParts[0].(map[string]any)["text"].(string)
	if strings.Contains(strings.ToLower(devText), "codex") {
		t.Fatalf("developer 文本块中的外来身份句未替换: %s", devText)
	}

	// 幂等性验证：二次处理不重复拼接前缀
	outBytes2 := PrepareOutboundChatBody(outBytes, "glm-5.3-flash")
	var out2 map[string]any
	_ = json.Unmarshal(outBytes2, &out2)
	sysContent2 := out2["messages"].([]any)[0].(map[string]any)["content"].(string)
	if strings.Count(sysContent2, SystemIdentityLine) != 1 {
		t.Fatalf("重复规整不应重复拼接身份前缀: %s", sysContent2)
	}
}

func TestPrepareOutboundChatBodyNoSystemAndNoMaxTokens(t *testing.T) {
	input := `{"model":"openclaw","messages":[{"role":"user","content":"hi"}]}`
	outBytes := PrepareOutboundChatBody([]byte(input), "openclaw")
	var out map[string]any
	_ = json.Unmarshal(outBytes, &out)
	if _, exists := out["max_tokens"]; exists {
		t.Fatalf("未传 max_tokens 时不应主动注入")
	}
	msgs := out["messages"].([]any)
	if len(msgs) != 2 {
		t.Fatalf("缺少 system 时应插入首条 system 消息，实际数量: %d", len(msgs))
	}
	first := msgs[0].(map[string]any)
	if first["role"] != "system" || first["content"] != SystemIdentityPrefix {
		t.Fatalf("插入的首条 system 消息内容不符: %+v", first)
	}
}

func TestClassifyErrorLevels(t *testing.T) {
	e1 := ClassifyError(200, 410000, "not logged in", 0)
	if e1.Kind != ErrorKindSessionDead {
		t.Fatalf("410000 应归类为会话失效，实际: %s", e1.Kind)
	}
	e2 := ClassifyError(402, 0, "余额不足", 0)
	if e2.Kind != ErrorKindHardCredit {
		t.Fatalf("402 应归类为积分不足长冷却，实际: %s", e2.Kind)
	}
	e3 := ClassifyError(429, 0, "rate limit", 15*time.Second)
	if e3.Kind != ErrorKindSoftRate || e3.RetryAfter != 15*time.Second {
		t.Fatalf("429 分类或 Retry-After 解析异常: %+v", e3)
	}
	if ParseRetryAfter("12") != 12*time.Second {
		t.Fatalf("Retry-After 秒数解析失败")
	}
}

func TestParseModelsConfigBothShapes(t *testing.T) {
	topLevel := `{"models":[{"id":"glm-5","name":"GLM 5","creditConsumptionLevel":"中","input":["text","image"],"reasoning":true}]}`
	m1, err := ParseModelsConfigJSON([]byte(topLevel), 200, RegionGlobal)
	if err != nil || len(m1) != 1 || m1[0].Region != RegionGlobal || m1[0].CreditLevel != "中" || !m1[0].SupportsImages || !m1[0].IsReasoning {
		t.Fatalf("顶层 models 解析失败: %+v, err=%v", m1, err)
	}

	wrapped := `{"code":0,"msg":"ok","data":{"models":[{"id":"cn-model-x","creditConsumptionLevel":2}]}}`
	m2, err := ParseModelsConfigJSON([]byte(wrapped), 200, RegionCN)
	if err != nil || len(m2) != 1 || m2[0].Region != RegionCN || m2[0].CreditLevel != "档位 2" {
		t.Fatalf("data.models 信封解析失败: %+v, err=%v", m2, err)
	}
}

func TestParseOAuthCallbackURL(t *testing.T) {
	p, code, state, ok := ParseOAuthCallbackURL("http://localhost:53699/auth/callback-zai?code=abc123&state=xyz")
	if !ok || p != OAuthProviderZai || code != "abc123" || state != "xyz" {
		t.Fatalf("Zai 回调解析失败: provider=%s code=%s state=%s ok=%v", p, code, state, ok)
	}
	_, _, _, okBad := ParseOAuthCallbackURL("https://evil.example.com/auth/callback-google?code=123")
	if okBad {
		t.Fatalf("非回环域名回调应被拒绝")
	}
}

// TestEnsureStreamUsageDefaults 验证流式请求会默认补齐 include_usage，
// 但客户端显式设置时不覆盖。
func TestEnsureStreamUsageDefaults(t *testing.T) {
	// 未显式声明 -> 自动补 true
	in := []byte(`{"model":"m","stream":true,"messages":[]}`)
	out := PrepareOutboundChatBody(in, "m")
	var got map[string]any
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	opts, _ := got["stream_options"].(map[string]any)
	if opts == nil || opts["include_usage"] != true {
		t.Fatalf("流式请求应默认补齐 include_usage=true，实际 %+v", got["stream_options"])
	}

	// 显式 false -> 尊重客户端
	in2 := []byte(`{"model":"m","stream":true,"stream_options":{"include_usage":false},"messages":[]}`)
	out2 := PrepareOutboundChatBody(in2, "m")
	var got2 map[string]any
	if err := json.Unmarshal(out2, &got2); err != nil {
		t.Fatal(err)
	}
	opts2, _ := got2["stream_options"].(map[string]any)
	if opts2["include_usage"] != false {
		t.Fatalf("客户端显式设置不应被覆盖，实际 %+v", opts2)
	}

	// 非流式不注入
	in3 := []byte(`{"model":"m","messages":[]}`)
	out3 := PrepareOutboundChatBody(in3, "m")
	var got3 map[string]any
	if err := json.Unmarshal(out3, &got3); err != nil {
		t.Fatal(err)
	}
	if _, exists := got3["stream_options"]; exists {
		t.Fatal("非流式请求不应注入 stream_options")
	}
}
