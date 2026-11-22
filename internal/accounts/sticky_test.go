package accounts

import (
	"testing"
	"time"
)

func TestStickyBinderLifecycle(t *testing.T) {
	s := NewStickyBinder()
	if _, ok := s.Lookup("k1"); ok {
		t.Fatal("初始不应有绑定")
	}
	s.Bind("k1", "acc-1")
	if id, ok := s.Lookup("k1"); !ok || id != "acc-1" {
		t.Fatalf("绑定后应命中 acc-1，实际 %q ok=%v", id, ok)
	}
	// 解除账号绑定
	s.DropAccount("acc-1")
	if _, ok := s.Lookup("k1"); ok {
		t.Fatal("DropAccount 后不应再命中")
	}
}

func TestStickyBinderExpiryAndCapacity(t *testing.T) {
	s := NewStickyBinder()
	s.Bind("k", "a")
	// 手工把过期时间提前，模拟 TTL 到期
	s.mu.Lock()
	e := s.items["k"]
	e.expiresAt = time.Now().Add(-time.Second)
	s.items["k"] = e
	s.mu.Unlock()
	if _, ok := s.Lookup("k"); ok {
		t.Fatal("过期绑定不应命中")
	}

	// 容量上限：写入超过上限后条目数不应无限增长
	for i := 0; i < stickyMaxEntries+500; i++ {
		s.Bind(StickyKey("conv", string(rune(i%2000))), "acc")
	}
	if n := s.Len(); n > stickyMaxEntries {
		t.Fatalf("条目数应受限于 %d，实际 %d", stickyMaxEntries, n)
	}
}

func TestStickyKeyDeterministic(t *testing.T) {
	a := StickyKey("prompt-cache-1", "")
	b := StickyKey("prompt-cache-1", "")
	if a == "" || a != b {
		t.Fatalf("同一显式键应得到相同结果: %q vs %q", a, b)
	}
	c := StickyKey("", "用户第一句话")
	d := StickyKey("", "用户第一句话")
	if c == "" || c != d {
		t.Fatalf("同一前缀应得到相同结果: %q vs %q", c, d)
	}
	if c == a {
		t.Fatal("显式键与内容哈希应可区分")
	}
	if StickyKey("", "") != "" {
		t.Fatal("无任何信息时应返回空键")
	}
}
