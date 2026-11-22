package gateway

import (
	"bytes"
	"errors"
	"io"
	"net/http"

	"autoclaw2api/internal/protocol"
	"autoclaw2api/internal/translator"
)

// handleAnthropicMessages 实现 Anthropic Messages API（POST /v1/messages），可直接接入 Claude Code 等客户端。
func (s *Server) handleAnthropicMessages(w http.ResponseWriter, r *http.Request) {
	rawBody, err := io.ReadAll(io.LimitReader(r.Body, 32<<20))
	if err != nil {
		writeAnthropicError(w, http.StatusBadRequest, "invalid_request_error", "读取请求体失败: "+err.Error())
		return
	}

	chatBody, model, stream, err := translator.AnthropicToOpenAIRequest(rawBody)
	if err != nil {
		writeAnthropicError(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	if model == "" {
		writeAnthropicError(w, http.StatusBadRequest, "invalid_request_error", "缺少必需字段 model")
		return
	}

	reqCtx := withStickyKey(r.Context(), extractStickyKey(rawBody))
	if stream {
		resp, prefix, _, err := s.executeChatStreamWithFailover(reqCtx, model, chatBody)
		if err != nil {
			writeAnthropicUpstreamError(w, err)
			return
		}
		defer func() { _ = resp.Body.Close() }()

		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)

		flusher, _ := w.(http.Flusher)
		conv := translator.NewAnthropicStreamConverter(model)
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
		// 建立连接阶段已读出首个数据块，与剩余流合并后再按行解析
		watched := withWatchdog(io.MultiReader(bytes.NewReader(prefix), resp.Body), resp.Body)
		defer watched.Stop()
		normalEnd := scanSSEDataTracked(watched.reader, emit)
		if !normalEnd {
			if s.logger != nil {
				s.logger.Warn("Gateway", "Anthropic 流式被中断（无 finish_reason/DONE）model=%s", model)
			}
			_, _ = w.Write([]byte("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"upstream stream interrupted before finish_reason\"}}\n\n"))
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

	payload, _, err := s.executeChatWithFailover(reqCtx, model, chatBody)
	if err != nil {
		writeAnthropicUpstreamError(w, err)
		return
	}
	converted, err := translator.OpenAIToAnthropicResponse(payload, model)
	if err != nil {
		writeAnthropicError(w, http.StatusBadGateway, "api_error", "转换 Anthropic 响应失败: "+err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(converted)
}

func writeAnthropicError(w http.ResponseWriter, status int, errType, message string) {
	writeJSON(w, status, map[string]any{
		"type": "error",
		"error": map[string]any{
			"type":    errType,
			"message": message,
		},
	})
}

func writeAnthropicUpstreamError(w http.ResponseWriter, err error) {
	var uerr *protocol.UpstreamError
	if errors.As(err, &uerr) {
		status := uerr.HTTPStatus
		if status <= 0 {
			status = http.StatusBadGateway
		}
		errType := "api_error"
		switch uerr.Kind {
		case protocol.ErrorKindSessionDead:
			errType = "authentication_error"
		case protocol.ErrorKindSoftRate:
			errType = "rate_limit_error"
		case protocol.ErrorKindClient:
			errType = "invalid_request_error"
		}
		writeAnthropicError(w, status, errType, uerr.Message)
		return
	}
	writeAnthropicError(w, http.StatusBadGateway, "api_error", err.Error())
}
