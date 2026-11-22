package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"autoclaw2api/internal/accounts"
)

// stickyProbe 只抽取会话粘性需要的字段，避免为整份请求体建完整对象树。
type stickyProbe struct {
	PromptCacheKey     string            `json:"prompt_cache_key"`
	PreviousResponseID string            `json:"previous_response_id"`
	Conversation       json.RawMessage   `json:"conversation"`
	Messages           []json.RawMessage `json:"messages"`
	Input              json.RawMessage   `json:"input"`
	System             json.RawMessage   `json:"system"`
}

// extractStickyKey 从请求体提取会话粘性键。
//
// 优先级：显式会话标识（prompt_cache_key / conversation / previous_response_id）
// 其次：首轮内容（system + 第一条 message / input）的前缀哈希。
// 拿不到任何信息时返回空串，表示该请求不做粘性绑定。
func extractStickyKey(rawBody []byte) string {
	if len(rawBody) == 0 {
		return ""
	}
	var p stickyProbe
	if err := json.Unmarshal(rawBody, &p); err != nil {
		return ""
	}

	explicit := strings.TrimSpace(p.PromptCacheKey)
	if explicit == "" {
		explicit = conversationID(p.Conversation)
	}
	if explicit == "" {
		explicit = strings.TrimSpace(p.PreviousResponseID)
	}

	first := firstRaw(p.System)
	if first == "" && len(p.Messages) > 0 {
		first = firstRaw(p.Messages[0])
	}
	if first == "" {
		first = firstRaw(p.Input)
	}
	return accounts.StickyKey(explicit, first)
}

// conversationID 兼容 conversation 为字符串或 {"id":"..."} 对象两种写法。
func conversationID(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return ""
	}
	if raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err == nil {
			return strings.TrimSpace(s)
		}
		return ""
	}
	var obj struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &obj); err == nil {
		return strings.TrimSpace(obj.ID)
	}
	return ""
}

// firstRaw 把 json.RawMessage 归一化成可哈希字符串：字符串取原文，其余取精简后的 JSON。
func firstRaw(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return ""
	}
	if raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err == nil {
			return s
		}
		return ""
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return strconv.Quote(string(raw))
	}
	return buf.String()
}

type stickyKeyCtx struct{}

// withStickyKey 把会话粘性键放入请求上下文，供账号选择阶段使用。
func withStickyKey(ctx context.Context, key string) context.Context {
	if key == "" {
		return ctx
	}
	return context.WithValue(ctx, stickyKeyCtx{}, key)
}

// stickyKeyFrom 取出上下文中的会话粘性键。
func stickyKeyFrom(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(stickyKeyCtx{}).(string); ok {
		return v
	}
	return ""
}
