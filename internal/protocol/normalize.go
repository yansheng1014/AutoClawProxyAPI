package protocol

import (
	"encoding/json"
	"strings"
)

const (
	// SystemIdentityLine 是上游预校验要求首条 system 消息必须以此开头的核心身份句。
	SystemIdentityLine = "You are a personal assistant running inside OpenClaw."

	// SystemIdentityPrefix 是完整的白名单前缀文本。
	SystemIdentityPrefix = SystemIdentityLine + "\n\n" +
		"## Tooling\n" +
		"Available tools are policy-filtered. Names are case-sensitive; call exactly as listed.\n"

	// MinAllowedMaxTokens 是上游允许的最小显式 max_tokens 阈值。
	MinAllowedMaxTokens = 10000
)

// identityRewriteRule 定义单条外来客户端身份描述到中性表述的改写规则。
type identityRewriteRule struct {
	pattern string
	neutral string
}

// promptSanitizeRules 按模式长度降序排列，防止较短规则优先匹配截断较长短语。
var promptSanitizeRules = []identityRewriteRule{
	{
		pattern: "You are a coding agent running in the Codex CLI tool",
		neutral: "You are a coding agent running in a terminal CLI tool",
	},
	{
		pattern: "You are running as a coding agent in the Codex CLI",
		neutral: "You are running as a coding agent in a terminal CLI",
	},
	{
		pattern: "You are an AI agent powered by DeepSeek Harness",
		neutral: "You are an AI agent powered by a local coding harness",
	},
	{
		pattern: "You are a coding agent running in the Codex CLI",
		neutral: "You are a coding agent running in a terminal CLI",
	},
	{
		pattern: "You are ZCode, an interactive coding agent",
		neutral: "You are an interactive coding agent",
	},
	{
		pattern: "You are Claude Code",
		neutral: "You are a coding assistant",
	},
	{
		pattern: "You are ZCode",
		neutral: "You are an interactive coding agent",
	},
	{
		pattern: "You are Codex",
		neutral: "You are a coding agent",
	},
}

// PrepareOutboundChatBody 对出站对话请求体执行防 406 规整：
//  1. 按路由结果覆盖 model，并删除上游不接受的 user 字段；
//  2. 将所有 system / developer 消息中的外来客户端身份句替换为中性描述；
//  3. 确保首条消息为 system 且内容以 SystemIdentityPrefix 开头（幂等不重复拼接）；
//  4. 若显式传入小于 10000 的正整数 max_tokens，则自动抬升至 10000；未传时不注入，不改动 max_completion_tokens。
func PrepareOutboundChatBody(rawBody []byte, targetModel string) []byte {
	if len(rawBody) == 0 {
		return rawBody
	}
	var payload map[string]any
	if err := json.Unmarshal(rawBody, &payload); err != nil {
		return rawBody
	}

	if m := strings.TrimSpace(targetModel); m != "" {
		payload["model"] = m
	}
	delete(payload, "user")

	normalizeMessagesForUpstream(payload)
	normalizeMaxTokensFloor(payload)
	ensureStreamUsage(payload)

	encoded, err := json.Marshal(payload)
	if err != nil {
		return rawBody
	}
	return encoded
}

func normalizeMessagesForUpstream(payload map[string]any) {
	msgs, ok := payload["messages"].([]any)
	if !ok {
		return
	}

	// 先改写所有 system / developer 消息中的敏感外来身份描述
	for _, item := range msgs {
		msg, ok := item.(map[string]any)
		if !ok || !isSystemOrDeveloper(msg) {
			continue
		}
		sanitizeMessageIdentities(msg)
	}

	// 检查首条消息是否已经是 system（若首条是 developer，也保留其位置并在前方插入标准 system，或将 system 前置）
	if len(msgs) > 0 {
		if first, ok := msgs[0].(map[string]any); ok && isSystemOrDeveloper(first) {
			// 若首条是 developer，上游要求首条 role 为 system，因此将其规范化为 system 并前置身份句
			if role, _ := first["role"].(string); strings.EqualFold(strings.TrimSpace(role), "developer") {
				first["role"] = "system"
			}
			ensureIdentityPrefix(first)
			return
		}
	}

	// 首条非 system 时在最前面插入一条标准 system 消息
	head := map[string]any{
		"role":    "system",
		"content": SystemIdentityPrefix,
	}
	payload["messages"] = append([]any{head}, msgs...)
}

func isSystemOrDeveloper(msg map[string]any) bool {
	role, ok := msg["role"].(string)
	if !ok {
		return false
	}
	r := strings.TrimSpace(role)
	return strings.EqualFold(r, "system") || strings.EqualFold(r, "developer")
}

func sanitizeMessageIdentities(msg map[string]any) {
	switch content := msg["content"].(type) {
	case string:
		msg["content"] = ReplaceForeignIdentities(content)
	case []any:
		for _, item := range content {
			part, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if text, ok := part["text"].(string); ok {
				part["text"] = ReplaceForeignIdentities(text)
			}
		}
	}
}

func ensureIdentityPrefix(msg map[string]any) {
	switch content := msg["content"].(type) {
	case string:
		if !strings.HasPrefix(content, SystemIdentityLine) {
			msg["content"] = joinIdentityPrefix(content)
		}
	case []any:
		for _, item := range content {
			part, ok := item.(map[string]any)
			if !ok {
				continue
			}
			text, ok := part["text"].(string)
			if !ok {
				continue
			}
			if !strings.HasPrefix(text, SystemIdentityLine) {
				part["text"] = joinIdentityPrefix(text)
			}
			return
		}
		// 数组中不存在 text 块时在头部补一个文本块
		msg["content"] = append([]any{map[string]any{
			"type": "text",
			"text": SystemIdentityPrefix,
		}}, content...)
	default:
		msg["content"] = SystemIdentityPrefix
	}
}

func joinIdentityPrefix(original string) string {
	if original == "" {
		return SystemIdentityPrefix
	}
	return SystemIdentityPrefix + "\n" + original
}

// ReplaceForeignIdentities 将文本中所有外来客户端身份句（不区分 ASCII 大小写）替换为中性描述。
func ReplaceForeignIdentities(text string) string {
	for _, rule := range promptSanitizeRules {
		text = replaceFoldAll(text, rule.pattern, rule.neutral)
	}
	return text
}

func replaceFoldAll(src, target, repl string) string {
	idx := indexFoldASCII(src, target)
	if idx < 0 {
		return src
	}
	var b strings.Builder
	b.Grow(len(src))
	for idx >= 0 {
		b.WriteString(src[:idx])
		b.WriteString(repl)
		src = src[idx+len(target):]
		idx = indexFoldASCII(src, target)
	}
	b.WriteString(src)
	return b.String()
}

func indexFoldASCII(s, substr string) int {
	n := len(substr)
	if n == 0 || n > len(s) {
		return -1
	}
	for i := 0; i <= len(s)-n; i++ {
		if strings.EqualFold(s[i:i+n], substr) {
			return i
		}
	}
	return -1
}

func normalizeMaxTokensFloor(payload map[string]any) {
	raw, exists := payload["max_tokens"]
	if !exists {
		return
	}
	v, ok := raw.(float64)
	if !ok {
		return
	}
	if v > 0 && v < MinAllowedMaxTokens && v == float64(int64(v)) {
		payload["max_tokens"] = float64(MinAllowedMaxTokens)
	}
}

// ensureStreamUsage 对 stream:true 的请求补齐 stream_options.include_usage。
//
// 上游支持在流式响应末尾回传精确 usage（含 cached_tokens）。若客户端未显式声明，
// 这里默认补上；客户端已显式设置（无论 true/false）则尊重其选择。
func ensureStreamUsage(payload map[string]any) {
	if stream, ok := payload["stream"].(bool); !ok || !stream {
		return
	}
	opts, ok := payload["stream_options"].(map[string]any)
	if !ok {
		opts = map[string]any{}
	}
	if _, exists := opts["include_usage"]; !exists {
		opts["include_usage"] = true
	}
	payload["stream_options"] = opts
}
