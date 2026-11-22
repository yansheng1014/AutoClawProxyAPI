package accounts

import (
	"hash/fnv"
	"sync"
	"time"
)

// 会话粘性（Prompt Cache 亲和）参数：
// 多轮对话若在多个账号之间来回轮转，上游前缀缓存命中率会大幅下降、积分消耗翻倍，
// 因此把同一会话固定绑定到同一账号一段时间。
const (
	stickyTTL         = 15 * time.Minute
	stickyMaxEntries  = 4096
	stickyPruneEveryN = 256
)

type stickyEntry struct {
	accountID string
	expiresAt time.Time
}

// StickyBinder 维护「会话键 -> 账号」的短时绑定，纯内存、有容量上限、惰性淘汰。
type StickyBinder struct {
	mu    sync.Mutex
	items map[string]stickyEntry
	ops   int
}

// NewStickyBinder 创建会话粘性绑定器。
func NewStickyBinder() *StickyBinder {
	return &StickyBinder{items: make(map[string]stickyEntry, 256)}
}

// Lookup 返回会话键当前绑定的账号 ID（已过期则视为未绑定）。
func (s *StickyBinder) Lookup(key string) (string, bool) {
	if key == "" {
		return "", false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.items[key]
	if !ok {
		return "", false
	}
	if time.Now().After(e.expiresAt) {
		delete(s.items, key)
		return "", false
	}
	return e.accountID, true
}

// Bind 将会话键绑定到账号，并顺带做惰性淘汰以控制内存占用。
func (s *StickyBinder) Bind(key, accountID string) {
	if key == "" || accountID == "" {
		return
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	s.items[key] = stickyEntry{accountID: accountID, expiresAt: now.Add(stickyTTL)}

	s.ops++
	if s.ops%stickyPruneEveryN != 0 && len(s.items) <= stickyMaxEntries {
		return
	}
	// 先清过期，再在必要时按任意顺序淘汰超出容量的条目
	for k, e := range s.items {
		if now.After(e.expiresAt) {
			delete(s.items, k)
		}
	}
	for k := range s.items {
		if len(s.items) <= stickyMaxEntries {
			break
		}
		delete(s.items, k)
	}
}

// DropAccount 清除某账号的全部绑定（该账号限流/余额不足/失效时调用，促使下次重新选号）。
func (s *StickyBinder) DropAccount(accountID string) {
	if accountID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, e := range s.items {
		if e.accountID == accountID {
			delete(s.items, k)
		}
	}
}

// Len 返回当前绑定条目数（测试与观察用）。
func (s *StickyBinder) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.items)
}

// StickyKey 依据请求体中的显式会话标识生成粘性键；
// 没有显式标识时用首轮内容哈希，保证相同对话前缀落到同一账号。
func StickyKey(explicit string, firstUserContent string) string {
	if explicit != "" {
		h := fnv.New64a()
		_, _ = h.Write([]byte(explicit))
		return "k" + u64hex(h.Sum64())
	}
	if firstUserContent == "" {
		return ""
	}
	// 只取前缀，避免长上下文带来的哈希开销
	const prefixLimit = 512
	if len(firstUserContent) > prefixLimit {
		firstUserContent = firstUserContent[:prefixLimit]
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(firstUserContent))
	return "h" + u64hex(h.Sum64())
}

func u64hex(v uint64) string {
	const digits = "0123456789abcdef"
	buf := make([]byte, 16)
	for i := 15; i >= 0; i-- {
		buf[i] = digits[v&0xf]
		v >>= 4
	}
	return string(buf)
}
