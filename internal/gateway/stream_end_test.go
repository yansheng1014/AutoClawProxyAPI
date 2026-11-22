package gateway

import (
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"autoclaw2api/internal/protocol"
)

// TestStreamPassthroughAbnormalEnd 验证：上游连接中途被掐断（无 finish_reason）时，
// 网关要补发标准错误事件，客户端因此能识别失败并重试，而不是只看到「流结束但没有 finish_reason」。
func TestStreamPassthroughAbnormalEnd(t *testing.T) {
	s := &Server{}
	rec := httptest.NewRecorder()
	pr, pw := io.Pipe()
	go func() {
		_, _ = pw.Write([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n"))
		_ = pw.CloseWithError(errors.New("connection reset by peer"))
	}()

	s.streamOpenAIPassthrough(rec, nil, pr, nil)

	body := rec.Body.String()
	if !strings.Contains(body, "stream_interrupted") {
		t.Fatalf("中断的流应补发 stream_interrupted 错误事件，实际: %s", body)
	}
}

// TestStreamPassthroughNormalEnd 验证：正常结束（出现 finish_reason / [DONE]）时不得补发错误事件。
func TestStreamPassthroughNormalEnd(t *testing.T) {
	s := &Server{}
	rec := httptest.NewRecorder()
	pr, pw := io.Pipe()
	go func() {
		_, _ = pw.Write([]byte("data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"))
		_, _ = pw.Write([]byte("data: [DONE]\n\n"))
		_ = pw.Close()
	}()

	s.streamOpenAIPassthrough(rec, nil, pr, nil)

	if body := rec.Body.String(); strings.Contains(body, "stream_interrupted") {
		t.Fatalf("正常结束不应补发错误事件，实际: %s", body)
	}
}

// TestStreamPassthroughIdleWatchdog 验证：长时间无数据时看门狗会断开，
// 且同样以错误事件收尾（而不是无声结束）。
func TestStreamPassthroughIdleWatchdog(t *testing.T) {
	if testing.Short() {
		t.Skip("依赖真实计时")
	}
	old := streamIdleTimeout
	streamIdleTimeout = 300 * time.Millisecond
	defer func() { streamIdleTimeout = old }()

	s := &Server{}
	rec := httptest.NewRecorder()
	pr, pw := io.Pipe()
	go func() {
		_, _ = pw.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"x\"},\"finish_reason\":null}]}\n\n"))
		// 之后一直不发数据，也不关闭
		time.Sleep(2 * time.Second)
		_ = pw.Close()
	}()

	s.streamOpenAIPassthrough(rec, nil, pr, nil)

	body := rec.Body.String()
	if !strings.Contains(body, "stream_interrupted") {
		t.Fatalf("静默超时应补发错误事件，实际: %s", body)
	}
}

// TestUpstreamTimeoutMapsTo504 验证：上游等待响应头超时要以 504 返回，
// 并且错误文案直接告诉调用方「换模型或稍后重试」，而不是含糊的 502 transient。
func TestUpstreamTimeoutMapsTo504(t *testing.T) {
	rec := httptest.NewRecorder()
	err := protocol.NewTransportTimeoutError("请求上游流式接口超时",
		errors.New("net/http: timeout awaiting response headers"))

	writeOpenAIUpstreamError(rec, err)

	if rec.Code != 504 {
		t.Fatalf("期望 504，实际 %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{"建议更换模型", "transient"} {
		if !strings.Contains(body, want) {
			t.Fatalf("响应缺少 %q：%s", want, body)
		}
	}
}
