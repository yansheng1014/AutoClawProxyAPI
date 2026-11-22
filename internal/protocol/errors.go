package protocol

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ErrorKind 定义上游错误的分级类别，供调度器决定冷却、禁用或切号重试策略。
type ErrorKind string

const (
	ErrorKindSessionDead ErrorKind = "session_dead" // 账号会话失效（410000/400000/登录过期）→ 暂停调度
	ErrorKindHardCredit  ErrorKind = "hard_credit"  // 积分或余额不足（HTTP 402/配额文案）→ 长冷却并切号
	ErrorKindSoftRate    ErrorKind = "soft_rate"    // 触发限流（HTTP 429）→ 短冷却并切号
	ErrorKindTransient   ErrorKind = "transient"    // 网络异常、超时或 5xx → 可退避重试与切号
	ErrorKindClient      ErrorKind = "client"       // 4xx 参数错误等 → 直接返回客户端
)

// UpstreamError 封装分类后的上游调用错误。
type UpstreamError struct {
	HTTPStatus int
	BizCode    int64
	Kind       ErrorKind
	Message    string
	RetryAfter time.Duration
}

func (e *UpstreamError) Error() string {
	if e.BizCode != 0 {
		return fmt.Sprintf("上游错误 [%s] (HTTP %d, code=%d): %s", e.Kind, e.HTTPStatus, e.BizCode, e.Message)
	}
	if e.HTTPStatus > 0 {
		return fmt.Sprintf("上游错误 [%s] (HTTP %d): %s", e.Kind, e.HTTPStatus, e.Message)
	}
	return fmt.Sprintf("上游错误 [%s]: %s", e.Kind, e.Message)
}

// ShouldFailover 判断该错误是否应当切换其他可用账号重试。
func (e *UpstreamError) ShouldFailover() bool {
	switch e.Kind {
	case ErrorKindSessionDead, ErrorKindHardCredit, ErrorKindSoftRate, ErrorKindTransient:
		return true
	default:
		return false
	}
}

// IsDeadBizCode 判断业务码是否为登录会话失效码。
func IsDeadBizCode(code int64) bool {
	return code == BizCodeNotLoggedIn || code == BizCodeAuthExpired
}

// errorPhraseRule 定义按响应文本关键词命中后的分级规则。
type errorPhraseRule struct {
	kind       ErrorKind
	httpStatus int
	phrases    []string
}

var errorClassifierTable = []errorPhraseRule{
	{
		kind:       ErrorKindHardCredit,
		httpStatus: http.StatusPaymentRequired,
		phrases: []string{
			"insufficient credit", "no credit", "credit exhausted", "out of credit",
			"quota exceeded", "quota exhaust", "payment required", "not enough credit",
			"insufficient balance", "balance not enough", "limit reached", "usage limit",
			"余额不足", "额度不足", "积分不足", "积分用完", "额度用尽", "没有积分", "配额不足",
		},
	},
	{
		kind:       ErrorKindSessionDead,
		httpStatus: http.StatusUnauthorized,
		phrases: []string{
			"not logged in", "unauthorized", "invalid token", "token expired",
			"login required", "session expired", "offline user session", "auth.inject failed",
			"未登录", "登录已过期", "请先登录", "token 失效", "登录失效",
		},
	},
}

// containsSessionDeadPhrase 检查上游文本中是否包含会话失效类提示语。
func containsSessionDeadPhrase(text string) bool {
	lower := strings.ToLower(text)
	for _, rule := range errorClassifierTable {
		if rule.kind != ErrorKindSessionDead {
			continue
		}
		for _, phrase := range rule.phrases {
			if strings.Contains(lower, phrase) {
				return true
			}
		}
	}
	return false
}

// ClassifyError 根据 HTTP 状态码、上游业务码、响应文本和 Retry-After 请求头完成分级分类。
func ClassifyError(httpStatus int, bizCode int64, body string, retryAfter time.Duration) *UpstreamError {
	msg := truncateText(strings.TrimSpace(body), 320)
	if msg == "" {
		msg = http.StatusText(httpStatus)
	}

	if IsDeadBizCode(bizCode) {
		return &UpstreamError{
			HTTPStatus: http.StatusUnauthorized,
			BizCode:    bizCode,
			Kind:       ErrorKindSessionDead,
			Message:    msg,
			RetryAfter: retryAfter,
		}
	}

	lower := strings.ToLower(body)
	for _, rule := range errorClassifierTable {
		for _, phrase := range rule.phrases {
			if strings.Contains(lower, phrase) {
				return &UpstreamError{
					HTTPStatus: rule.httpStatus,
					BizCode:    bizCode,
					Kind:       rule.kind,
					Message:    msg,
					RetryAfter: retryAfter,
				}
			}
		}
	}

	switch {
	case httpStatus == http.StatusUnauthorized || httpStatus == http.StatusForbidden:
		return &UpstreamError{
			HTTPStatus: httpStatus,
			BizCode:    bizCode,
			Kind:       ErrorKindSessionDead,
			Message:    msg,
			RetryAfter: retryAfter,
		}
	case httpStatus == http.StatusPaymentRequired:
		return &UpstreamError{
			HTTPStatus: httpStatus,
			BizCode:    bizCode,
			Kind:       ErrorKindHardCredit,
			Message:    msg,
			RetryAfter: retryAfter,
		}
	case httpStatus == http.StatusTooManyRequests:
		return &UpstreamError{
			HTTPStatus: httpStatus,
			BizCode:    bizCode,
			Kind:       ErrorKindSoftRate,
			Message:    msg,
			RetryAfter: retryAfter,
		}
	case httpStatus >= 500 || httpStatus == http.StatusRequestTimeout || httpStatus == 0:
		return &UpstreamError{
			HTTPStatus: httpStatus,
			BizCode:    bizCode,
			Kind:       ErrorKindTransient,
			Message:    msg,
			RetryAfter: retryAfter,
		}
	default:
		return &UpstreamError{
			HTTPStatus: httpStatus,
			BizCode:    bizCode,
			Kind:       ErrorKindClient,
			Message:    msg,
			RetryAfter: retryAfter,
		}
	}
}

// ParseRetryAfter 解析 HTTP Retry-After 响应头（支持秒数和 RFC1123 日期格式）。
func ParseRetryAfter(headerVal string) time.Duration {
	v := strings.TrimSpace(headerVal)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		d := time.Until(t)
		if d > 0 {
			return d
		}
	}
	return 0
}

// DetectEnvelopeError 检测 JSON 业务信封 {"code": 非0, "msg": ...}。
func DetectEnvelopeError(body []byte) (int64, string, bool) {
	var env struct {
		Code    *int64 `json:"code"`
		Msg     string `json:"msg"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return 0, "", false
	}
	if env.Code == nil || *env.Code == 0 {
		return 0, "", false
	}
	msg := strings.TrimSpace(env.Msg)
	if msg == "" {
		msg = strings.TrimSpace(env.Message)
	}
	return *env.Code, msg, true
}

func truncateText(s string, maxRunes int) string {
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	return string(runes[:maxRunes]) + "..."
}

// IsTransportTimeout 判断错误是否为网络层超时（含响应头等待超时、连接/读写超时）。
// 这类错误通常意味着上游排队或链路拥塞，换账号重试往往无效，应快速失败并提示换模型。
func IsTransportTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "timeout awaiting response headers")
}

// NewTransportTimeoutError 构造上游超时错误（HTTP 504，语义比 502 更准确）。
func NewTransportTimeoutError(prefix string, err error) *UpstreamError {
	return &UpstreamError{
		HTTPStatus: http.StatusGatewayTimeout,
		Kind:       ErrorKindTransient,
		Message:    prefix + "：上游迟迟未返回响应头（模型排队或过载），建议更换模型或稍后重试（" + err.Error() + "）",
	}
}
