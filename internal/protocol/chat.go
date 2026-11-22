package protocol

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

const defaultChatTimeout = 10 * time.Minute

// ChatRequest 定义向 AutoClaw 上游发起对话补全的参数。
type ChatRequest struct {
	BaseURL string
	Token   string
	Model   string
	Body    []byte
}

// ChatCompletion 执行非流式对话补全，返回上游原始 OpenAI 兼容 JSON 响应。
func ChatCompletion(ctx context.Context, client *http.Client, req ChatRequest) ([]byte, error) {
	if client == nil {
		client = &http.Client{Timeout: defaultChatTimeout}
	}
	endpoint := req.BaseURL + PathChat
	prepared := PrepareOutboundChatBody(req.Body, req.Model)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(prepared))
	if err != nil {
		return nil, fmt.Errorf("构造对话请求失败: %w", err)
	}
	AttachChatHeaders(httpReq, req.Token, req.Model)

	resp, err := client.Do(httpReq)
	if err != nil {
		if IsTransportTimeout(err) {
			return nil, NewTransportTimeoutError("请求上游对话接口超时", err)
		}
		return nil, &UpstreamError{
			Kind:    ErrorKindTransient,
			Message: "请求上游对话接口失败: " + err.Error(),
		}
	}
	defer func() { _ = resp.Body.Close() }()

	retryAfter := ParseRetryAfter(resp.Header.Get("Retry-After"))
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, &UpstreamError{
			HTTPStatus: resp.StatusCode,
			Kind:       ErrorKindTransient,
			Message:    "读取上游对话响应失败: " + err.Error(),
			RetryAfter: retryAfter,
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, ClassifyError(resp.StatusCode, 0, string(body), retryAfter)
	}
	if code, msg, isErr := DetectEnvelopeError(body); isErr {
		return nil, ClassifyError(resp.StatusCode, code, msg, retryAfter)
	}
	return body, nil
}

// ChatCompletionStream 发起流式 SSE 对话请求，成功时返回开放的 *http.Response（由调用方关闭 Body）。
func ChatCompletionStream(ctx context.Context, client *http.Client, req ChatRequest) (*http.Response, error) {
	if client == nil {
		client = &http.Client{}
	}
	endpoint := req.BaseURL + PathChat
	prepared := PrepareOutboundChatBody(req.Body, req.Model)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(prepared))
	if err != nil {
		return nil, fmt.Errorf("构造流式对话请求失败: %w", err)
	}
	AttachChatHeaders(httpReq, req.Token, req.Model)

	resp, err := client.Do(httpReq)
	if err != nil {
		if IsTransportTimeout(err) {
			return nil, NewTransportTimeoutError("请求上游流式接口超时", err)
		}
		return nil, &UpstreamError{
			Kind:    ErrorKindTransient,
			Message: "请求上游流式接口失败: " + err.Error(),
		}
	}
	retryAfter := ParseRetryAfter(resp.Header.Get("Retry-After"))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer func() { _ = resp.Body.Close() }()
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return nil, ClassifyError(resp.StatusCode, 0, string(raw), retryAfter)
	}
	// 检查上游是否以 HTTP 200 返回了 JSON 业务错误信封（非 text/event-stream）
	ct := resp.Header.Get("Content-Type")
	if ct != "" && !bytes.Contains([]byte(ct), []byte("event-stream")) {
		raw, errRead := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		_ = resp.Body.Close()
		if errRead != nil {
			return nil, &UpstreamError{
				HTTPStatus: resp.StatusCode,
				Kind:       ErrorKindTransient,
				Message:    "读取上游非流式错误响应失败: " + errRead.Error(),
			}
		}
		if code, msg, isErr := DetectEnvelopeError(raw); isErr {
			return nil, ClassifyError(resp.StatusCode, code, msg, retryAfter)
		}
		// 即使 Content-Type 不是 event-stream，若内容是合法流式或 JSON，包装回 Body 给调用方处理
		resp.Body = io.NopCloser(bytes.NewReader(raw))
	}
	return resp, nil
}
