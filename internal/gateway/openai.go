package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"autoclaw2api/internal/accounts"
	"autoclaw2api/internal/protocol"
	"autoclaw2api/internal/translator"
)

// handleListModels 实现 OpenAI 兼容的 GET /v1/models。
func (s *Server) handleListModels(w http.ResponseWriter, _ *http.Request) {
	items := s.router.ListOpenAIModels()
	now := time.Now().Unix()
	data := make([]map[string]any, 0, len(items))
	for _, m := range items {
		owner := "autoclaw2api"
		if m.Channel != "" {
			owner = m.Channel
		}
		data = append(data, map[string]any{
			"id":       m.ID,
			"object":   "model",
			"created":  now,
			"owned_by": owner,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"object": "list",
		"data":   data,
	})
}

// handleChatCompletions 实现 OpenAI 兼容的 POST /v1/chat/completions（流式与非流式）。
func (s *Server) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	rawBody, err := io.ReadAll(io.LimitReader(r.Body, 32<<20))
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request_error", "读取请求体失败: "+err.Error())
		return
	}

	var meta struct {
		Model  string `json:"model"`
		Stream bool   `json:"stream"`
	}
	if err := json.Unmarshal(rawBody, &meta); err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request_error", "请求体不是合法 JSON: "+err.Error())
		return
	}
	if strings.TrimSpace(meta.Model) == "" {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request_error", "缺少必需字段 model")
		return
	}

	if meta.Stream {
		start := time.Now()
		resp, prefix, usedAcc, err := s.executeChatStreamWithFailover(r.Context(), meta.Model, rawBody)
		firstAt := time.Since(start)
		if err != nil {
			writeOpenAIUpstreamError(w, err)
			return
		}
		defer func() { _ = resp.Body.Close() }()
		s.streamOpenAIPassthrough(w, prefix, resp.Body, usedAcc)
		s.logSlowRequest(meta.Model, usedAcc, firstAt, time.Since(start))
		return
	}

	payload, _, err := s.executeChatWithFailover(r.Context(), meta.Model, rawBody)
	if err != nil {
		writeOpenAIUpstreamError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(payload)
}

// handleResponses 实现 OpenAI 兼容的 POST /v1/responses（含非流式与流式事件转换）。
func (s *Server) handleResponses(w http.ResponseWriter, r *http.Request) {
	rawBody, err := io.ReadAll(io.LimitReader(r.Body, 32<<20))
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request_error", "读取请求体失败: "+err.Error())
		return
	}

	chatBody, model, stream, err := translator.ResponsesToOpenAIRequest(rawBody)
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	if model == "" {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request_error", "缺少必需字段 model")
		return
	}

	if stream {
		resp, prefix, _, err := s.executeChatStreamWithFailover(r.Context(), model, chatBody)
		if err != nil {
			writeOpenAIUpstreamError(w, err)
			return
		}
		defer func() { _ = resp.Body.Close() }()

		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)

		flusher, _ := w.(http.Flusher)
		conv := translator.NewResponsesStreamConverter(model)
		emit := func(data []byte) bool {
			for _, frame := range conv.ProcessSSEData(data) {
				if _, err := w.Write(frame); err != nil {
					return false
				}
			}
			if flusher != nil {
				flusher.Flush()
			}
			return true
		}
		watched := withWatchdog(io.MultiReader(bytes.NewReader(prefix), resp.Body), resp.Body)
		defer watched.Stop()
		normalEnd := scanSSEDataTracked(watched.reader, emit)
		if !normalEnd {
			if s.logger != nil {
				s.logger.Warn("Gateway", "Responses 流式被中断（无 finish_reason/DONE）model=%s", model)
			}
			_, _ = w.Write([]byte("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"upstream_stream_error\",\"message\":\"upstream stream interrupted before finish_reason\"}}\n\n"))
			if flusher != nil {
				flusher.Flush()
			}
			return
		}
		for _, frame := range conv.Finish() {
			_, _ = w.Write(frame)
		}
		if flusher != nil {
			flusher.Flush()
		}
		return
	}

	payload, _, err := s.executeChatWithFailover(r.Context(), model, chatBody)
	if err != nil {
		writeOpenAIUpstreamError(w, err)
		return
	}
	converted, err := translator.OpenAIToResponsesResponse(payload, model)
	if err != nil {
		writeOpenAIError(w, http.StatusBadGateway, "server_error", "转换 Responses 响应失败: "+err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(converted)
}

// watchedStream 在读取上游流的过程中维护空闲看门狗。
type watchedStream struct {
	reader io.Reader
	stop   func()
}

// Stop 结束看门狗。
func (w *watchedStream) Stop() { w.stop() }

type watchdogReader struct {
	r  io.Reader
	wd *streamWatchdog
}

func (r *watchdogReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n > 0 {
		r.wd.Touch()
	}
	return n, err
}

// withWatchdog 包装上游响应体：超过 streamIdleTimeout 没有新数据就关闭连接，
// 让阻塞中的读立即返回，避免客户端无限期等待（表现为“卡住几分钟”）。
func withWatchdog(r io.Reader, closer io.Closer) *watchedStream {
	return withWatchdogCallback(r, closer, nil)
}

// withWatchdogCallback 同上，但允许在超时后先执行回调（用于记录日志、通知客户端），再关闭连接。
func withWatchdogCallback(r io.Reader, closer io.Closer, onIdle func()) *watchedStream {
	wd := newStreamWatchdog(streamIdleTimeout, func() {
		if onIdle != nil {
			onIdle()
		}
		_ = closer.Close()
	})
	return &watchedStream{reader: &watchdogReader{r: r, wd: wd}, stop: wd.Stop}
}

func (s *Server) streamOpenAIPassthrough(w http.ResponseWriter, prefix []byte, body io.ReadCloser, acc *accounts.Account) {
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	flusher, _ := w.(http.Flusher)
	stream := io.MultiReader(bytes.NewReader(prefix), body)
	started := time.Now()
	watched := withWatchdogCallback(stream, body, func() {
		if s.logger != nil {
			s.logger.Warn("Gateway", "上游流式静默超过 %s，主动断开连接（已持续 %s）", streamIdleTimeout, time.Since(started).Round(time.Second))
		}
	})
	defer watched.Stop()
	reader := bufio.NewReaderSize(watched.reader, 64*1024)
	// 只有出现过 finish_reason 或 [DONE] 才算正常结束；
	// 否则（连接被掐断/上游中断）要显式告诉客户端，避免只看到「流结束但没有 finish_reason」。
	normalEnd := false
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			if bytes.Contains(line, []byte("[DONE]")) {
				normalEnd = true
			} else if bytes.Contains(line, []byte("\"finish_reason\"")) && !bytes.Contains(line, []byte("\"finish_reason\":null")) {
				normalEnd = true
			}
			if _, wErr := w.Write(line); wErr != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if err != nil {
			if !normalEnd && err != io.EOF {
				// 连接被中断：补一个标准错误事件，客户端可据此重试
				_, _ = w.Write([]byte("data: {\"error\":{\"message\":\"upstream stream interrupted before finish_reason\",\"type\":\"upstream_stream_error\",\"code\":\"stream_interrupted\"}}\n\n"))
				if flusher != nil {
					flusher.Flush()
				}
			}
			if !normalEnd {
				label := "unknown"
				if acc != nil {
					label = acc.Cred.DisplayLabel()
				}
				if s.logger != nil {
					s.logger.Warn("Gateway", "上游流式异常结束（无 finish_reason/DONE）账号=[%s] 已持续=%s 原因=%v",
						label, time.Since(started).Round(time.Millisecond), err)
				}
			}
			return
		}
	}
}

// executeChatWithFailover 执行带指数退避与自动切号故障转移的非流式对话请求。
func (s *Server) executeChatWithFailover(ctx context.Context, requestedModel string, chatBody []byte) ([]byte, *accounts.Account, error) {
	targets, err := s.router.ResolveTargets(requestedModel)
	if err != nil {
		return nil, nil, &protocol.UpstreamError{
			HTTPStatus: http.StatusBadRequest,
			Kind:       protocol.ErrorKindClient,
			Message:    err.Error(),
		}
	}

	retryCfg := s.cfgMgr.Get().Retry
	maxAttempts := max(1, retryCfg.MaxRetries+1)
	triedAccounts := make(map[string]bool)
	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		acc, target, ok := s.pickNextAccount(targets, triedAccounts)
		if !ok {
			if lastErr != nil {
				return nil, nil, lastErr
			}
			return nil, nil, &protocol.UpstreamError{
				HTTPStatus: http.StatusServiceUnavailable,
				Kind:       protocol.ErrorKindTransient,
				Message:    fmt.Sprintf("模型 %s 暂无可用就绪账号（请检查账号池状态或稍后重试）", requestedModel),
			}
		}
		triedAccounts[acc.ID] = true

		baseURL := s.baseURLFor(target.Region)
		client, errClient := s.proxyMgr.ClientForAccount(acc.ID, string(target.Region), 10*time.Minute)
		if errClient != nil {
			lastErr = &protocol.UpstreamError{
				HTTPStatus: http.StatusBadGateway,
				Kind:       protocol.ErrorKindTransient,
				Message:    "代理出口不可用: " + errClient.Error(),
			}
			if s.logger != nil {
				s.logger.Warn("Gateway", "账号 [%s] 代理出口不可用，自动切号: %v", acc.Cred.DisplayLabel(), errClient)
			}
			continue
		}
		respBytes, errCall := protocol.ChatCompletion(ctx, client, protocol.ChatRequest{
			BaseURL: baseURL,
			Token:   acc.Cred.AccessToken,
			Model:   target.UpstreamModel,
			Body:    chatBody,
		})
		if errCall == nil {
			s.pool.ReportSuccess(acc.ID)
			return respBytes, acc, nil
		}

		lastErr = errCall
		var uerr *protocol.UpstreamError
		if errors.As(errCall, &uerr) {
			s.pool.ReportError(acc.ID, uerr)
			if s.logger != nil {
				s.logger.Warn("Gateway", "账号 [%s] (%s) 调用模型 %s 失败 [%s]: %s",
					acc.Cred.DisplayLabel(), target.Region, target.UpstreamModel, uerr.Kind, uerr.Message)
			}
			if !uerr.ShouldFailover() {
				return nil, nil, uerr
			}
			// 还有候选账号可切号时，限流/积分不足/失效立即切号；仅在瞬态网络错误或重试时短暂退避
			if attempt+1 < maxAttempts {
				if uerr.Kind == protocol.ErrorKindTransient {
					if !sleepWithContext(ctx, calcBackoff(attempt, retryCfg, uerr.RetryAfter)) {
						return nil, nil, ctx.Err()
					}
				}
				continue
			}
		}
		return nil, nil, lastErr
	}
	return nil, nil, lastErr
}

// 流式响应的看门狗参数：
//   - firstChunkTimeout：上游返回 200 后迟迟不出第一个数据块，视为该出口不可用，直接切号重试
//   - streamIdleTimeout：流式过程中长时间没有新数据（上游卡死/连接被中间设备黑洞），主动断开
//
// 长思考模型可能几十秒不吐字，静默阈值过小会误杀正常的长时间推理，
// 这里给到 5 分钟；真正的「上游已死」由 TCP 层与客户端超时兜底。
var (
	firstChunkTimeout = 45 * time.Second
	streamIdleTimeout = 5 * time.Minute
)

// errFirstChunkTimeout 表示等待上游首包超时。
var errFirstChunkTimeout = errors.New("等待上游首包超时")

// readFirstChunk 在 d 时间内读取上游响应的第一个数据块。
// 超时返回 errFirstChunkTimeout；调用方应先关闭 resp.Body 以解除阻塞中的读。
func readFirstChunk(r io.Reader, d time.Duration) ([]byte, error) {
	type result struct {
		buf []byte
		err error
	}
	ch := make(chan result, 1)
	go func() {
		buf := make([]byte, 8192)
		n, err := r.Read(buf)
		ch <- result{buf[:n], err}
	}()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case res := <-ch:
		if len(res.buf) > 0 {
			return append([]byte(nil), res.buf...), nil
		}
		if res.err != nil {
			return nil, res.err
		}
		return nil, io.EOF
	case <-timer.C:
		return nil, errFirstChunkTimeout
	}
}

// executeChatStreamWithFailover 执行带自动切号故障转移的流式 SSE 请求。
// 返回的 prefix 是已经从上游读出的首个数据块，调用方需要在写响应头后先写它。
func (s *Server) executeChatStreamWithFailover(ctx context.Context, requestedModel string, chatBody []byte) (*http.Response, []byte, *accounts.Account, error) {
	targets, err := s.router.ResolveTargets(requestedModel)
	if err != nil {
		return nil, nil, nil, &protocol.UpstreamError{
			HTTPStatus: http.StatusBadRequest,
			Kind:       protocol.ErrorKindClient,
			Message:    err.Error(),
		}
	}

	retryCfg := s.cfgMgr.Get().Retry
	maxAttempts := max(1, retryCfg.MaxRetries+1)
	triedAccounts := make(map[string]bool)
	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		acc, target, ok := s.pickNextAccount(targets, triedAccounts)
		if !ok {
			if lastErr != nil {
				return nil, nil, nil, lastErr
			}
			return nil, nil, nil, &protocol.UpstreamError{
				HTTPStatus: http.StatusServiceUnavailable,
				Kind:       protocol.ErrorKindTransient,
				Message:    fmt.Sprintf("模型 %s 暂无可用就绪账号（请检查账号池状态或稍后重试）", requestedModel),
			}
		}
		triedAccounts[acc.ID] = true

		baseURL := s.baseURLFor(target.Region)
		client, errClient := s.proxyMgr.ClientForAccount(acc.ID, string(target.Region), 0)
		if errClient != nil {
			lastErr = &protocol.UpstreamError{
				HTTPStatus: http.StatusBadGateway,
				Kind:       protocol.ErrorKindTransient,
				Message:    "代理出口不可用: " + errClient.Error(),
			}
			if s.logger != nil {
				s.logger.Warn("Gateway", "账号 [%s] 流式代理出口不可用，自动切号: %v", acc.Cred.DisplayLabel(), errClient)
			}
			continue
		}
		resp, errCall := protocol.ChatCompletionStream(ctx, client, protocol.ChatRequest{
			BaseURL: baseURL,
			Token:   acc.Cred.AccessToken,
			Model:   target.UpstreamModel,
			Body:    chatBody,
		})
		if errCall == nil {
			// 真正成功要等「第一个数据块」到达：上游可能已返回 200 却长时间不出字
			prefix, errFirst := readFirstChunk(resp.Body, firstChunkTimeout)
			if errFirst == nil {
				s.pool.ReportSuccess(acc.ID)
				return resp, prefix, acc, nil
			}
			_ = resp.Body.Close()
			lastErr = &protocol.UpstreamError{
				Kind:    protocol.ErrorKindTransient,
				Message: fmt.Sprintf("上游首包未到达（%v）", errFirst),
			}
			s.pool.ReportError(acc.ID, lastErr.(*protocol.UpstreamError))
			if s.logger != nil {
				s.logger.Warn("Gateway", "账号 [%s] (%s) 模型 %s 首包超时/中断，自动切号: %v",
					acc.Cred.DisplayLabel(), target.Region, target.UpstreamModel, errFirst)
			}
			if attempt+1 < maxAttempts {
				// 已经等过一轮首包超时，直接切号不再叠加退避
				continue
			}
			return nil, nil, nil, lastErr
		}

		lastErr = errCall
		var uerr *protocol.UpstreamError
		if errors.As(errCall, &uerr) {
			s.pool.ReportError(acc.ID, uerr)
			if s.logger != nil {
				s.logger.Warn("Gateway", "账号 [%s] (%s) 流式调用模型 %s 失败 [%s]: %s",
					acc.Cred.DisplayLabel(), target.Region, target.UpstreamModel, uerr.Kind, uerr.Message)
			}
			if !uerr.ShouldFailover() {
				return nil, nil, nil, uerr
			}
			if attempt+1 < maxAttempts {
				if uerr.Kind == protocol.ErrorKindTransient {
					if !sleepWithContext(ctx, calcBackoff(attempt, retryCfg, uerr.RetryAfter)) {
						return nil, nil, nil, ctx.Err()
					}
				}
				continue
			}
		}
		return nil, nil, nil, lastErr
	}
	return nil, nil, nil, lastErr
}

// logSlowRequest 只对慢请求打印分段耗时，便于定位卡在「路由/连接/首包/流式」哪个环节。
func (s *Server) logSlowRequest(model string, acc *accounts.Account, firstChunk, total time.Duration) {
	if s.logger == nil || total < 5*time.Second {
		return
	}
	label := "unknown"
	if acc != nil {
		label = acc.Cred.DisplayLabel()
	}
	s.logger.Warn("Perf", "慢请求 model=%s 账号=[%s] 首包=%dms 总耗时=%dms 流式=%dms",
		model, label, firstChunk.Milliseconds(), total.Milliseconds(),
		(total - firstChunk).Milliseconds())
}

// streamWatchdog 在流式传输过程中监控「多久没有新数据」，
// 超过阈值就主动关闭上游连接，避免客户端无限期挂起。
type streamWatchdog struct {
	mu      sync.Mutex
	last    time.Time
	stopped bool
	stopCh  chan struct{}
}

func newStreamWatchdog(idle time.Duration, onIdle func()) *streamWatchdog {
	w := &streamWatchdog{last: time.Now(), stopCh: make(chan struct{})}
	interval := idle / 3
	if interval < time.Second {
		interval = time.Second
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-w.stopCh:
				return
			case <-t.C:
				w.mu.Lock()
				idleFor := time.Since(w.last)
				stopped := w.stopped
				w.mu.Unlock()
				if stopped {
					return
				}
				if idleFor >= idle {
					onIdle()
					return
				}
			}
		}
	}()
	return w
}

// Touch 记录一次数据到达。
func (w *streamWatchdog) Touch() {
	w.mu.Lock()
	w.last = time.Now()
	w.mu.Unlock()
}

// Stop 结束监控。
func (w *streamWatchdog) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.stopped {
		w.stopped = true
		close(w.stopCh)
	}
}

func (s *Server) pickNextAccount(targets []accounts.RouteTarget, exclude map[string]bool) (*accounts.Account, accounts.RouteTarget, bool) {
	for _, target := range targets {
		if acc, ok := s.pool.Acquire(target.Region, exclude); ok {
			return acc, target, true
		}
	}
	return nil, accounts.RouteTarget{}, false
}

// scanSSEData 逐行读取上游 SSE 数据流并回调每条 data 载荷。
// scanSSEDataTracked 与 scanSSEData 相同，但额外报告上游是否正常结束
// （出现过 finish_reason 或 [DONE]），用于识别连接被中途掐断的情况。
func scanSSEDataTracked(body io.Reader, onData func([]byte) bool) bool {
	normalEnd := false
	scanSSEData(body, func(data []byte) bool {
		if bytes.Contains(data, []byte("[DONE]")) ||
			(bytes.Contains(data, []byte("\"finish_reason\"")) && !bytes.Contains(data, []byte("\"finish_reason\":null"))) {
			normalEnd = true
		}
		return onData(data)
	})
	return normalEnd
}

func scanSSEData(body io.Reader, onData func([]byte) bool) {
	reader := bufio.NewReaderSize(body, 64*1024)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			trimmed := bytes.TrimSpace(line)
			if bytes.HasPrefix(trimmed, []byte("data:")) {
				payload := bytes.TrimSpace(trimmed[5:])
				if len(payload) > 0 {
					if !onData(payload) {
						return
					}
				}
			}
		}
		if err != nil {
			return
		}
	}
}

func writeOpenAIError(w http.ResponseWriter, status int, errType, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]any{
			"message": message,
			"type":    errType,
		},
	})
}

func writeOpenAIUpstreamError(w http.ResponseWriter, err error) {
	var uerr *protocol.UpstreamError
	if errors.As(err, &uerr) {
		status := uerr.HTTPStatus
		if status <= 0 {
			status = http.StatusBadGateway
		}
		writeOpenAIError(w, status, string(uerr.Kind), uerr.Message)
		return
	}
	writeOpenAIError(w, http.StatusBadGateway, "upstream_error", err.Error())
}
