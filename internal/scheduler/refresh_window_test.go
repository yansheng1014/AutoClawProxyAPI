package scheduler

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"autoclaw2api/internal/accounts"
)

// TestRefreshWindow 验证续期窗口按 token 真实生命周期动态计算：
// 24h 的国际版令牌不应再用 72h 阈值（否则每 15 分钟就续期一次）。
func TestRefreshWindow(t *testing.T) {
	// 构造 24h 生命周期令牌：iat=now-23h, exp=now+1h
	now := time.Now()
	exp := now.Add(time.Hour).Unix()
	iat := exp - 86400
	tok := fakeJWT(t, map[string]any{"iat": iat, "exp": exp})

	win := refreshWindow(accounts.Credential{AccessToken: tok, ExpiresAt: exp})
	want := 4 * time.Hour // ttl/6 = 24h/6
	if win != want {
		t.Fatalf("24h 令牌续期窗口应为 %s，实际 %s", want, win)
	}

	// 无法解析生命周期时回落 72h
	if got := refreshWindow(accounts.Credential{}); got != 72*time.Hour {
		t.Fatalf("无生命周期信息应回落 72h，实际 %s", got)
	}
}

func TestRefreshBackoffEscalates(t *testing.T) {
	prev := time.Duration(0)
	for i := 1; i <= 8; i++ {
		d := refreshBackoffDelay(i)
		if d < prev {
			t.Fatalf("退避应单调不减，第 %d 次 %s < 前一次 %s", i, d, prev)
		}
		prev = d
	}
	if refreshBackoffDelay(8) != 12*time.Hour {
		t.Fatalf("退避上限应为 12h，实际 %s", refreshBackoffDelay(8))
	}
}

func fakeJWT(t *testing.T, claims map[string]any) string {
	t.Helper()
	raw, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	return "eyJhbGciOiJub25lIn0." + base64.RawURLEncoding.EncodeToString(raw) + ".sig"
}
