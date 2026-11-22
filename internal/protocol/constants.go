package protocol

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Region 表示 AutoClaw 部署区域。
type Region string

const (
	RegionCN     Region = "cn"
	RegionGlobal Region = "global"
)

// ChannelPrefix 定义双区渠道标识（同时作为凭证文件名前缀和定点路由前缀）。
const (
	ChannelCN     = "autoclawcn"
	ChannelGlobal = "autoclawglobal"
	SourceID      = "autoclaw"
)

// 双区默认上游基地址。
const (
	DefaultBaseURLCN     = "https://autoglm-acceleration-api.zhipuai.cn"
	DefaultBaseURLGlobal = "https://autoglm-api.autoglm.ai"
)

// 上游业务端点路径。
const (
	PathChat             = "/autoclaw-proxy/proxy/autoclaw/v1/chat/completions"
	PathModelsConfig     = "/autoclaw-proxy/proxy/autoclaw-model-config"
	PathRefreshToken     = "/userapi/v1/refresh"
	PathWallets          = "/agent-assetmgr/api/v2/wallets?biz_app_id=autoclaw"
	PathDailyCheckin     = "/autoclaw-proxy/proxy/autoclaw-task-complete"
	PathUserInfo         = "/userapi/v1/user-info"
	PathSMSSendCode      = "/userapi/v1/agent-send-code"
	PathSMSLogin         = "/userapi/v1/agent-login/" // 末尾斜杠必须保留
	PathOAuthCaptchaConf = "/userapi/overseasv1/oauth-captcha-config"
	PathOAuthURLZai      = "/userapi/overseasv1/zai-oauth-url"
	PathOAuthURLGoogle   = "/userapi/overseasv1/google-oauth-url"
	PathOAuthLoginZai    = "/userapi/overseasv1/zai-oauth-login"
	PathOAuthLoginGoogle = "/userapi/overseasv1/google-oauth-login"
)

// 官方客户端协议签名与指纹常量。
const (
	AuthAppID      = "100003"
	AuthSignSecret = "38d2391985e2369a5fb8227d8e6cd5e5"
	ClientVersion  = "1.18.5"
	ClientUA       = "AutoClaw/" + ClientVersion

	BizCodeNotLoggedIn = 410000
	BizCodeAuthExpired = 400000

	// DefaultTokenTTL 是上游未显式返回 expires_in 时的令牌保守有效时长（28 天）。
	DefaultTokenTTL = 28 * 24 * time.Hour
)

// IsGlobal 判断区域是否为国际版。
func (r Region) IsGlobal() bool {
	return r == RegionGlobal
}

// Channel 返回区域对应的渠道前缀标识。
func (r Region) Channel() string {
	if r.IsGlobal() {
		return ChannelGlobal
	}
	return ChannelCN
}

// DisplayName 返回区域的中文展示名称。
func (r Region) DisplayName() string {
	if r.IsGlobal() {
		return "国际版 (Global)"
	}
	return "国内版 (CN)"
}

// NormalizeRegion 归一化区域字符串，未知值回落至国内版。
func NormalizeRegion(raw string) Region {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "global", "intl", "international", ChannelGlobal:
		return RegionGlobal
	default:
		return RegionCN
	}
}

// ResolveBaseURL 返回指定区域的生效基地址（支持配置覆盖）。
func ResolveBaseURL(region Region, override string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(override), "/")
	if trimmed != "" {
		return trimmed
	}
	if region.IsGlobal() {
		return DefaultBaseURLGlobal
	}
	return DefaultBaseURLCN
}

// CalcAuthSign 计算 UserAPI 与 OAuth 请求签名：MD5("100003&<ts>&38d2391985e2369a5fb8227d8e6cd5e5")。
func CalcAuthSign(ts string) string {
	raw := AuthAppID + "&" + ts + "&" + AuthSignSecret
	sum := md5.Sum([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// RandomHex 生成指定字符长度的随机十六进制字符串。
func RandomHex(hexLen int) string {
	if hexLen <= 0 {
		return ""
	}
	byteLen := (hexLen + 1) / 2
	buf := make([]byte, byteLen)
	if _, err := rand.Read(buf); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	s := hex.EncodeToString(buf)
	if len(s) > hexLen {
		return s[:hexLen]
	}
	return s
}

func formatBearer(token string) string {
	t := strings.TrimSpace(token)
	if t == "" {
		return ""
	}
	if strings.HasPrefix(strings.ToLower(t), "bearer ") {
		return t
	}
	return "Bearer " + t
}

func desktopTM() string {
	if runtime.GOOS == "windows" {
		return "win"
	}
	return "linux"
}

// AttachChatHeaders 设置对话补全专用请求头（第一套独立请求头）。
// 携带双通道 Bearer、X-Request-Model 与桌面端 mac 指纹；两个 trace 头复用同一次随机值。
func AttachChatHeaders(req *http.Request, token, model string) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("User-Agent", ClientUA)
	req.Header.Set("X-Tm", "mac")
	req.Header.Set("X-Version", ClientVersion)
	req.Header.Set("X-Product", SourceID)
	req.Header.Set("X-Channel", "official")
	req.Header.Set("X-Lang", "zh-CN")
	req.Header.Set("X-Client-Type", "pc")
	// 两个 trace 头使用同一个随机串，避免暴露第三方工具特征
	traceID := RandomHex(16)
	req.Header.Set("X-Trace-Id", traceID)
	req.Header.Set("x_trace_id", traceID)
	if b := formatBearer(token); b != "" {
		req.Header.Set("Authorization", b)
		req.Header.Set("X-Authorization", b)
	}
	if m := strings.TrimSpace(model); m != "" {
		req.Header.Set("X-Request-Model", m)
	}
}

// AttachUserHeaders 设置 UserAPI 业务请求头（第二套独立请求头）。
// 用于 Token 刷新、余额查询、每日签到、活跃上报、模型配置与国内短信登录。
func AttachUserHeaders(req *http.Request, token string) {
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("User-Agent", ClientUA)
	req.Header.Set("X-Version", ClientVersion)
	req.Header.Set("X-Tm", desktopTM())
	req.Header.Set("X-Product", SourceID)
	req.Header.Set("X-Client-Type", "pc")
	req.Header.Set("X-Harness-Type", "zcode")
	req.Header.Set("X-Channel", "official")
	req.Header.Set("X-Lang", "zh-CN")
	req.Header.Set("X-Auth-Appid", AuthAppID)
	req.Header.Set("X-Auth-TimeStamp", ts)
	req.Header.Set("X-Auth-Sign", CalcAuthSign(ts))
	req.Header.Set("X-Trace-Id", RandomHex(16))
	if b := formatBearer(token); b != "" {
		req.Header.Set("Authorization", b)
	}
}

// AttachOAuthHeaders 设置国际版 OAuth 专用请求头（第三套独立请求头）。
// 固定 X-Tm: linux 并携带签名与官方 UA，避免触发风控 WAF 405 拦截。
func AttachOAuthHeaders(req *http.Request, token string) {
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("User-Agent", ClientUA)
	req.Header.Set("X-Version", ClientVersion)
	req.Header.Set("X-Tm", "linux")
	req.Header.Set("X-Product", SourceID)
	req.Header.Set("X-Client-Type", "pc")
	req.Header.Set("X-Harness-Type", "zcode")
	req.Header.Set("X-Channel", "official")
	req.Header.Set("X-Lang", "zh-CN")
	req.Header.Set("X-Auth-Appid", AuthAppID)
	req.Header.Set("X-Auth-TimeStamp", ts)
	req.Header.Set("X-Auth-Sign", CalcAuthSign(ts))
	req.Header.Set("X-Trace-Id", RandomHex(16))
	if b := formatBearer(token); b != "" {
		req.Header.Set("Authorization", b)
	}
}

// ParseJWTClaims 尝试解析 JWT 令牌载荷（非 JWT 时返回 nil）。
func ParseJWTClaims(token string) map[string]any {
	parts := strings.Split(strings.TrimSpace(token), ".")
	if len(parts) < 2 {
		return nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil
	}
	return claims
}

// JWTExpirySeconds 从 JWT 的 exp 字段读取过期 Unix 秒数，无法解析返回 0。
func JWTExpirySeconds(token string) int64 {
	claims := ParseJWTClaims(token)
	if claims == nil {
		return 0
	}
	if v, ok := claims["exp"].(float64); ok && v > 0 {
		return int64(v)
	}
	return 0
}

// JWTIssuedAtSeconds 从 JWT 的 iat 字段读取签发时间，无法解析返回 0。
func JWTIssuedAtSeconds(token string) int64 {
	claims := ParseJWTClaims(token)
	if claims == nil {
		return 0
	}
	if v, ok := claims["iat"].(float64); ok && v > 0 {
		return int64(v)
	}
	return 0
}

// JWTUserID 从 JWT 载荷提取常见用户标识字段。
func JWTUserID(token string) string {
	claims := ParseJWTClaims(token)
	if claims == nil {
		return ""
	}
	for _, k := range []string{"user_id", "userId", "uid", "sub"} {
		if v, ok := claims[k]; ok {
			if s := StringFromAny(v); s != "" {
				return s
			}
		}
	}
	return ""
}

// StringFromAny 将 JSON 解析出的字符串或数字转换为去空白字符串。
func StringFromAny(v any) string {
	switch val := v.(type) {
	case string:
		return strings.TrimSpace(val)
	case float64:
		if val <= 0 {
			return ""
		}
		return strconv.FormatInt(int64(val), 10)
	case int64:
		if val <= 0 {
			return ""
		}
		return strconv.FormatInt(val, 10)
	case json.Number:
		return strings.TrimSpace(val.String())
	default:
		return ""
	}
}

// FloatFromAny 将 JSON 解析出的数字或数字字符串安全转换为 float64。
func FloatFromAny(v any) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case int64:
		return float64(val)
	case int:
		return float64(val)
	case json.Number:
		f, _ := val.Float64()
		return f
	case string:
		f, _ := strconv.ParseFloat(strings.TrimSpace(val), 64)
		return f
	default:
		return 0
	}
}
