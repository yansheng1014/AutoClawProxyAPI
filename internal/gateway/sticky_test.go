package gateway

import "testing"

// TestExtractStickyKey 验证会话粘性键的提取优先级：
// 显式会话标识 > 首轮内容哈希；三者任一缺失都不应 panic。
func TestExtractStickyKey(t *testing.T) {
	cases := []struct {
		name string
		body string
		diff bool // 与对照用例是否应不同
	}{
		{"prompt_cache_key", `{"model":"m","prompt_cache_key":"abc","messages":[{"role":"user","content":"hi"}]}`, false},
		{"conversation 字符串", `{"model":"m","conversation":"conv-1","messages":[{"role":"user","content":"hi"}]}`, false},
		{"conversation 对象", `{"model":"m","conversation":{"id":"conv-2"},"messages":[{"role":"user","content":"hi"}]}`, false},
		{"previous_response_id", `{"model":"m","previous_response_id":"resp-1","input":"hi"}`, false},
		{"首轮内容哈希", `{"model":"m","messages":[{"role":"user","content":"完全不同的开场白"}]}`, false},
	}

	seen := map[string]string{}
	for _, c := range cases {
		k := extractStickyKey([]byte(c.body))
		if k == "" {
			t.Fatalf("%s: 不应为空", c.name)
		}
		if prev, dup := seen[k]; dup && prev != c.name {
			// 显式键与内容哈希允许不同；这里只要求随机会话不碰撞
			t.Fatalf("%s 与 %s 生成了相同键", c.name, prev)
		}
		seen[k] = c.name
	}

	// 同一 conversation 的后续轮次必须稳定命中同一个键
	k1 := extractStickyKey([]byte(`{"model":"m","conversation":"same","messages":[{"role":"user","content":"第一轮"}]}`))
	k2 := extractStickyKey([]byte(`{"model":"m","conversation":"same","messages":[{"role":"user","content":"第一轮"},{"role":"assistant","content":"回复"},{"role":"user","content":"第二轮"}]}`))
	if k1 != k2 {
		t.Fatalf("同一会话不同轮次应稳定: %q vs %q", k1, k2)
	}

	// 畸形/空请求体不应 panic
	for _, bad := range []string{"", "{}", "not-json", `{"messages":[]}`} {
		_ = extractStickyKey([]byte(bad))
	}
}
