package protocol

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"testing"
)

func TestIsTransportTimeout(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"上下文超时", context.DeadlineExceeded, true},
		{"包装后的上下文超时", fmt.Errorf("wrap: %w", context.DeadlineExceeded), true},
		{"net.Error 超时", &fakeTimeoutErr{}, true},
		{"响应头等待超时", errors.New(`Post "https://x": net/http: timeout awaiting response headers`), true},
		{"普通连接错误", errors.New("connection refused"), false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		if got := IsTransportTimeout(c.err); got != c.want {
			t.Fatalf("%s: 期望 %v 实际 %v", c.name, c.want, got)
		}
	}
}

func TestNewTransportTimeoutError(t *testing.T) {
	e := NewTransportTimeoutError("请求上游流式接口超时", errors.New("timeout awaiting response headers"))
	if e.HTTPStatus != http.StatusGatewayTimeout {
		t.Fatalf("超时应映射为 504，实际 %d", e.HTTPStatus)
	}
	if e.Kind != ErrorKindTransient {
		t.Fatalf("超时应为 transient，实际 %s", e.Kind)
	}
	if !e.ShouldFailover() {
		t.Fatal("超时应允许切号重试")
	}
}

type fakeTimeoutErr struct{}

func (f *fakeTimeoutErr) Error() string   { return "i/o timeout" }
func (f *fakeTimeoutErr) Timeout() bool   { return true }
func (f *fakeTimeoutErr) Temporary() bool { return true }

var _ net.Error = (*fakeTimeoutErr)(nil)
