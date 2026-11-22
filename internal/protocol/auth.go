package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	OAuthPortZai               = 53699
	OAuthPortGoogle            = 46952
	loopbackCallbackPathPrefix = "/auth/callback-"
)

// OAuthProvider 表示国际版 OAuth 提供方（zai 或 google）。
type OAuthProvider string

const (
	OAuthProviderZai    OAuthProvider = "zai"
	OAuthProviderGoogle OAuthProvider = "google"
)

// NormalizeOAuthProvider 归一化 OAuth 渠道名称。
func NormalizeOAuthProvider(raw string) (OAuthProvider, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "zai":
		return OAuthProviderZai, nil
	case "", "google":
		return OAuthProviderGoogle, nil
	default:
		return "", fmt.Errorf("不支持的国际版登录渠道: %s", raw)
	}
}

// DefaultNavigateURI 生成国际版 OAuth 回环回调地址。
func DefaultNavigateURI(provider OAuthProvider) string {
	port := OAuthPortGoogle
	if provider == OAuthProviderZai {
		port = OAuthPortZai
	}
	return fmt.Sprintf("http://localhost:%d%s%s", port, loopbackCallbackPathPrefix, provider)
}

// LoginAccountData 承载短信或 OAuth 登录成功后换得的账号凭证数据。
type LoginAccountData struct {
	Region       Region
	AccessToken  string
	RefreshToken string
	ExpiresAt    int64
	UserID       string
	UserName     string
	Phone        string
	DeviceID     string
}

// CaptchaConfig 描述阿里云滑块人机验证初始化参数。
type CaptchaConfig struct {
	Enabled bool   `json:"enabled"`
	Region  string `json:"region"`
	Prefix  string `json:"prefix"`
	SceneID string `json:"scene_id"`
}

// NormalizeCNPhone 校验并归一化中国大陆 11 位手机号。
func NormalizeCNPhone(raw string) (string, error) {
	var b strings.Builder
	for _, r := range raw {
		if r != ' ' && r != '-' {
			b.WriteRune(r)
		}
	}
	s := strings.TrimPrefix(b.String(), "+86")
	if rest, ok := strings.CutPrefix(s, "86"); ok && len(rest) == 11 {
		s = rest
	}
	if len(s) != 11 || s[0] != '1' || s[1] < '2' || s[1] > '9' {
		return "", errors.New("请填写有效的 11 位中国大陆手机号")
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return "", errors.New("请填写有效的 11 位中国大陆手机号")
		}
	}
	return s, nil
}

// ParseSMSCode 校验 6 位数字短信验证码并转为整型（上游要求 JSON 数字类型）。
func ParseSMSCode(raw string) (int64, error) {
	trimmed := strings.TrimSpace(raw)
	if len(trimmed) != 6 {
		return 0, errors.New("请填写 6 位数字短信验证码")
	}
	val, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return 0, errors.New("短信验证码必须为 6 位纯数字")
	}
	return val, nil
}

// SendSMSCode 向国内版手机号发送短信验证码，返回后续登录必须复用的 device_id。
func SendSMSCode(ctx context.Context, client *http.Client, baseURL, phone string) (string, error) {
	normalized, err := NormalizeCNPhone(phone)
	if err != nil {
		return "", err
	}
	deviceID := RandomHex(32)
	raw, err := postAnonymousJSON(ctx, client, baseURL, PathSMSSendCode, map[string]any{
		"phone":     normalized,
		"source_id": SourceID,
		"device_id": deviceID,
	}, AttachUserHeaders)
	if err != nil {
		return "", err
	}
	code, msg, data := unpackLoginEnvelope(raw)
	if code != 0 {
		return "", errors.New(explainLoginBizError(false, code, msg))
	}
	var res struct {
		Result bool `json:"result"`
	}
	_ = json.Unmarshal(data, &res)
	if !res.Result {
		return "", errors.New("验证码发送失败，请稍后重试")
	}
	return deviceID, nil
}

// VerifySMSLogin 使用手机号、6 位数字验证码和同一次发送返回的 device_id 完成国内版登录。
func VerifySMSLogin(ctx context.Context, client *http.Client, baseURL, phone, codeStr, deviceID string) (*LoginAccountData, error) {
	normalized, err := NormalizeCNPhone(phone)
	if err != nil {
		return nil, err
	}
	codeNum, err := ParseSMSCode(codeStr)
	if err != nil {
		return nil, err
	}
	deviceID = strings.TrimSpace(deviceID)
	if deviceID == "" {
		deviceID = RandomHex(32)
	}
	raw, err := postAnonymousJSON(ctx, client, baseURL, PathSMSLogin, map[string]any{
		"phone":     normalized,
		"code":      codeNum,
		"platform":  "web",
		"source_id": SourceID,
		"device_id": deviceID,
	}, AttachUserHeaders)
	if err != nil {
		return nil, err
	}
	bizCode, msg, data := unpackLoginEnvelope(raw)
	if bizCode != 0 {
		return nil, errors.New(explainLoginBizError(false, bizCode, msg))
	}
	acc, err := decodeLoginAccount(data, RegionCN, deviceID)
	if err != nil {
		return nil, err
	}
	acc.Phone = normalized
	return acc, nil
}

// FetchOAuthCaptchaConfig 获取国际版阿里云人机验证配置。
func FetchOAuthCaptchaConfig(ctx context.Context, client *http.Client, baseURL string) (*CaptchaConfig, error) {
	raw, err := postAnonymousJSON(ctx, client, baseURL, PathOAuthCaptchaConf, map[string]any{}, AttachOAuthHeaders)
	if err != nil {
		return nil, err
	}
	bizCode, msg, data := unpackLoginEnvelope(raw)
	if bizCode != 0 {
		return nil, errors.New(explainLoginBizError(true, bizCode, msg))
	}
	var conf CaptchaConfig
	if err := json.Unmarshal(data, &conf); err != nil {
		return nil, fmt.Errorf("解析人机验证配置失败: %w", err)
	}
	conf.Region = strings.TrimSpace(conf.Region)
	conf.Prefix = strings.TrimSpace(conf.Prefix)
	conf.SceneID = strings.TrimSpace(conf.SceneID)
	return &conf, nil
}

// RequestOAuthURL 用阿里云滑块验证结果换取国际版 Zai / Google 授权链接。
func RequestOAuthURL(ctx context.Context, client *http.Client, baseURL string, provider OAuthProvider, navigateURI, captchaParam, deviceID string) (string, error) {
	captcha := strings.TrimSpace(captchaParam)
	if captcha == "" {
		return "", errors.New("请先完成阿里云滑块人机验证，再获取授权链接")
	}
	path := PathOAuthURLGoogle
	if provider == OAuthProviderZai {
		path = PathOAuthURLZai
	}
	raw, err := postAnonymousJSON(ctx, client, baseURL, path, map[string]any{
		"source_id":                SourceID,
		"device_id":                strings.TrimSpace(deviceID),
		"navigate_uri":             strings.TrimSpace(navigateURI),
		"ali_captcha_verify_param": captcha,
	}, AttachOAuthHeaders)
	if err != nil {
		return "", err
	}
	bizCode, msg, data := unpackLoginEnvelope(raw)
	if bizCode != 0 {
		return "", errors.New(explainLoginBizError(true, bizCode, msg))
	}
	var res struct {
		OAuthURL string `json:"oauth_url"`
	}
	if err := json.Unmarshal(data, &res); err != nil {
		return "", fmt.Errorf("解析授权地址响应失败: %w", err)
	}
	if strings.TrimSpace(res.OAuthURL) == "" {
		return "", errors.New("上游未返回有效授权链接，请重试")
	}
	return strings.TrimSpace(res.OAuthURL), nil
}

// ExchangeOAuthCode 用回环回调链接里的 code 与 state 换取国际版账号凭证。
func ExchangeOAuthCode(ctx context.Context, client *http.Client, baseURL string, provider OAuthProvider, code, state, navigateURI, deviceID string) (*LoginAccountData, error) {
	path := PathOAuthLoginGoogle
	if provider == OAuthProviderZai {
		path = PathOAuthLoginZai
	}
	raw, err := postAnonymousJSON(ctx, client, baseURL, path, map[string]any{
		"source_id":    SourceID,
		"device_id":    strings.TrimSpace(deviceID),
		"code":         strings.TrimSpace(code),
		"state":        strings.TrimSpace(state),
		"navigate_uri": strings.TrimSpace(navigateURI),
	}, AttachOAuthHeaders)
	if err != nil {
		return nil, err
	}
	bizCode, msg, data := unpackLoginEnvelope(raw)
	if bizCode != 0 {
		return nil, errors.New(explainOAuthCallbackBizError(bizCode, msg))
	}
	return decodeLoginAccount(data, RegionGlobal, deviceID)
}

// ParseOAuthCallbackURL 解析用户粘贴的回环回调 URL，提取提供方、code 与 state。
func ParseOAuthCallbackURL(raw string) (provider OAuthProvider, code, state string, ok bool) {
	s := strings.TrimSpace(raw)
	if idx := strings.IndexAny(s, "\r\n"); idx >= 0 {
		s = s[:idx]
	}
	parsed, err := url.Parse(s)
	if err != nil {
		return "", "", "", false
	}
	switch strings.ToLower(parsed.Hostname()) {
	case "localhost", "127.0.0.1", "::1":
	default:
		return "", "", "", false
	}
	if !strings.HasPrefix(parsed.Path, loopbackCallbackPathPrefix) {
		return "", "", "", false
	}
	suffix := strings.TrimPrefix(parsed.Path, loopbackCallbackPathPrefix)
	switch suffix {
	case string(OAuthProviderZai):
		provider = OAuthProviderZai
	case string(OAuthProviderGoogle):
		provider = OAuthProviderGoogle
	default:
		return "", "", "", false
	}
	q := parsed.Query()
	code = strings.TrimSpace(q.Get("code"))
	state = strings.TrimSpace(q.Get("state"))
	return provider, code, state, code != ""
}

func postAnonymousJSON(ctx context.Context, client *http.Client, baseURL, path string, body any, applyHeaders func(*http.Request, string)) ([]byte, error) {
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("序列化登录请求失败: %w", err)
	}
	endpoint := strings.TrimRight(baseURL, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(encoded))
	if err != nil {
		return nil, fmt.Errorf("构造登录请求失败: %w", err)
	}
	applyHeaders(req, "")

	resp, err := client.Do(req)
	if err != nil {
		return nil, &UpstreamError{
			Kind:    ErrorKindTransient,
			Message: "请求上游登录接口失败: " + err.Error(),
		}
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return nil, &UpstreamError{
			HTTPStatus: resp.StatusCode,
			Kind:       ErrorKindTransient,
			Message:    "读取上游登录响应失败: " + err.Error(),
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, ClassifyError(resp.StatusCode, 0, string(raw), ParseRetryAfter(resp.Header.Get("Retry-After")))
	}
	return raw, nil
}

func unpackLoginEnvelope(raw []byte) (int64, string, json.RawMessage) {
	var env struct {
		Code *int64          `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return 0, "", json.RawMessage("null")
	}
	var code int64
	if env.Code != nil {
		code = *env.Code
	}
	if len(env.Data) == 0 {
		env.Data = json.RawMessage("null")
	}
	return code, env.Msg, env.Data
}

func decodeLoginAccount(data json.RawMessage, region Region, deviceID string) (*LoginAccountData, error) {
	var payload struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		UserID       any    `json:"user_id"`
		UserIDCamel  any    `json:"userId"`
		UserName     string `json:"user_name"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, fmt.Errorf("解析登录结果失败: %w", err)
	}
	access := strings.TrimSpace(payload.AccessToken)
	if access == "" {
		return nil, errors.New("登录成功但上游未返回 access_token")
	}
	expiresAt := JWTExpirySeconds(access)
	if expiresAt == 0 {
		expiresAt = time.Now().Add(DefaultTokenTTL).Unix()
	}
	uid := StringFromAny(payload.UserID)
	if uid == "" {
		uid = StringFromAny(payload.UserIDCamel)
	}
	if uid == "" {
		uid = JWTUserID(access)
	}
	return &LoginAccountData{
		Region:       region,
		AccessToken:  access,
		RefreshToken: strings.TrimSpace(payload.RefreshToken),
		ExpiresAt:    expiresAt,
		UserID:       uid,
		UserName:     strings.TrimSpace(payload.UserName),
		DeviceID:     deviceID,
	}, nil
}

// explainOAuthCallbackBizError 解释「换票阶段」（ExchangeOAuthCode）的业务错误。
//
// 该阶段不涉及滑块验证，但上游在触发风控/审核失败时同样会返回 630014，
// 因此不能复用滑块阶段文案，否则会把真实的「审核失败」误报成「滑块未通过」。
// 所有分支都保留上游原始 code 与 msg，便于排查。
func explainOAuthCallbackBizError(code int64, upstreamMsg string) string {
	raw := strings.TrimSpace(upstreamMsg)
	tail := ""
	if raw != "" {
		tail = fmt.Sprintf("（code=%d，%s）", code, raw)
	} else {
		tail = fmt.Sprintf("（code=%d）", code)
	}
	switch code {
	case 630014:
		return "换票阶段被上游风控/审核拦截（常见原因：同一出口 IP 连续登录多个新号、浏览器与换票请求出口 IP 不一致、或该账号审核未通过）" + tail
	case 631002:
		return "换票阶段的授权校验未通过，请重新发起授权" + tail
	case 631001:
		return "回调授权码无效或已过期，请重新发起授权" + tail
	case 400002:
		return "请求签名校验失败（请检查本机系统时间是否准确）" + tail
	}
	return "换票失败" + tail
}

func explainLoginBizError(isOAuth bool, code int64, upstreamMsg string) string {
	switch code {
	case 400002:
		return "请求签名校验失败（请检查本机系统时间是否准确）"
	case 631002:
		if isOAuth {
			return "风控滑块验证未通过或已过期，请重新完成人机验证"
		}
		return "上游要求过风控验证，建议改用「导入凭证 JSON」方式添加"
	case 630014:
		if isOAuth {
			return "人机验证未通过，请重新滑动验证"
		}
		return "触发上游风控校验，请稍后重试或改用凭证 JSON 导入"
	case 631001:
		if isOAuth {
			return "回调授权码无效或已过期，请重新发起授权"
		}
	case 400001:
		if !isOAuth {
			return "请求参数有误，请检查手机号与 6 位数字验证码"
		}
	case 630202:
		if !isOAuth {
			return "短信验证码不正确，请核对后重试"
		}
	case 630101:
		if !isOAuth {
			return "短信验证码发送过于频繁，请稍后再试"
		}
	case BizCodeNotLoggedIn, BizCodeAuthExpired:
		return "登录状态已失效，请重新登录"
	}
	if msg := strings.TrimSpace(upstreamMsg); msg != "" {
		return "登录失败：" + msg
	}
	return fmt.Sprintf("登录失败（上游业务码 %d）", code)
}
