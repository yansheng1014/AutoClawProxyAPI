package config

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Version 声明当前程序版本号。
const Version = "v1.0.0"

// PanelAuth 保存 Web 控制台登录密码的安全哈希（不存明文）。
type PanelAuth struct {
	PasswordHash string `json:"password_hash,omitempty"`
}

// Initialized 报告是否已设置面板登录密码。
func (p PanelAuth) Initialized() bool {
	return strings.TrimSpace(p.PasswordHash) != ""
}

// HashPanelPassword 使用随机盐与 SHA-256 生成面板密码存储哈希。
func HashPanelPassword(plain string) (string, error) {
	if len(strings.TrimSpace(plain)) < 4 {
		return "", errors.New("面板密码长度不能少于 4 位")
	}
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("生成密码盐值失败: %w", err)
	}
	saltHex := hex.EncodeToString(salt)
	sum := sha256.Sum256([]byte(saltHex + ":" + plain))
	return saltHex + "$" + hex.EncodeToString(sum[:]), nil
}

// VerifyPanelPassword 使用常量时间比较校验明文密码与哈希是否一致。
func VerifyPanelPassword(plain, storedHash string) bool {
	storedHash = strings.TrimSpace(storedHash)
	if storedHash == "" || plain == "" {
		return false
	}
	parts := strings.SplitN(storedHash, "$", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return false
	}
	sum := sha256.Sum256([]byte(parts[0] + ":" + plain))
	gotHex := hex.EncodeToString(sum[:])
	return subtle.ConstantTimeCompare([]byte(gotHex), []byte(parts[1])) == 1
}

// APIKeyItem 描述一条独立的网关访问密钥记录。
type APIKeyItem struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Key       string `json:"key"`
	Enabled   bool   `json:"enabled"`
	CreatedAt string `json:"created_at"`
}

const vKeyAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// GenerateVKey 随机生成以 "vKey-" 开头的网关访问密钥。
func GenerateVKey() string {
	const bodyLen = 28
	buf := make([]byte, bodyLen)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("vKey-%x", time.Now().UnixNano())
	}
	out := make([]byte, bodyLen)
	for i, b := range buf {
		out[i] = vKeyAlphabet[int(b)%len(vKeyAlphabet)]
	}
	return "vKey-" + string(out)
}

// GenerateItemID 生成短随机标识符。
func GenerateItemID(prefix string) string {
	buf := make([]byte, 6)
	_, _ = rand.Read(buf)
	return prefix + "_" + hex.EncodeToString(buf)
}

// Config 定义网关全局配置结构。
type Config struct {
	Listen    string         `json:"listen"`
	PanelAuth PanelAuth      `json:"panel_auth"`
	APIKeys   []APIKeyItem   `json:"api_keys"`
	AuthDir   string         `json:"auth_dir"`
	DataDir   string         `json:"data_dir"`
	LogLevel  string         `json:"log_level"`
	Proxy     ProxyConfig    `json:"proxy,omitempty"`
	Upstream  UpstreamConfig `json:"upstream"`
	Retry     RetryConfig    `json:"retry"`
	Checkin   CheckinConfig  `json:"checkin"`
	Routing   RoutingConfig  `json:"routing"`
}

// ProxyConfig 保留用于兼容迁移旧版静态代理配置，运行期统一由代理池管理。
type ProxyConfig struct {
	Default string `json:"default,omitempty"`
	CN      string `json:"cn,omitempty"`
	Global  string `json:"global,omitempty"`
}

// UpstreamConfig 定义双区上游基地址覆盖项。
type UpstreamConfig struct {
	CNBaseURL     string `json:"cn_base_url"`
	GlobalBaseURL string `json:"global_base_url"`
}

// RetryConfig 定义指数退避重试与分级冷却时长参数。
type RetryConfig struct {
	MaxRetries       int `json:"max_retries"`
	BaseBackoffMs    int `json:"base_backoff_ms"`
	MaxBackoffMs     int `json:"max_backoff_ms"`
	ShortCooldownSec int `json:"short_cooldown_sec"`
	LongCooldownSec  int `json:"long_cooldown_sec"`
}

// CheckinConfig 定义每日定时自动签到参数。
type CheckinConfig struct {
	Enabled bool   `json:"enabled"`
	Time    string `json:"time"` // 24 小时制 HH:MM，未配置随机窗口时的固定时刻（兼容旧配置）
	// WindowStart / WindowEnd 定义每日随机签到的时段（本地时区 HH:MM）。
	// 配置后每天在该时段内按日期确定性随机一个时刻执行，避免固定时刻的脚本化特征。
	WindowStart string `json:"window_start"`
	WindowEnd   string `json:"window_end"`
}

// ParseHHMM 解析 "HH:MM" 为当日分钟数（0-1439）。
func ParseHHMM(v string) (int, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, false
	}
	parts := strings.SplitN(v, ":", 2)
	if len(parts) != 2 {
		return 0, false
	}
	h, errH := strconv.Atoi(strings.TrimSpace(parts[0]))
	m, errM := strconv.Atoi(strings.TrimSpace(parts[1]))
	if errH != nil || errM != nil || h < 0 || h > 23 || m < 0 || m > 59 {
		return 0, false
	}
	return h*60 + m, true
}

// CheckinWindow 返回生效的随机签到时段（分钟数，起 < 止）；未配置时返回 false。
func (c CheckinConfig) CheckinWindow() (int, int, bool) {
	start, okS := ParseHHMM(c.WindowStart)
	end, okE := ParseHHMM(c.WindowEnd)
	if !okS || !okE || end <= start {
		return 0, 0, false
	}
	return start, end, true
}

// RoutingConfig 定义模型发现、别名与启停路由策略。
type RoutingConfig struct {
	PrefixMode     string              `json:"prefix_mode"` // bare | prefixed | both
	DisabledModels []string            `json:"disabled_models"`
	Aliases        map[string][]string `json:"aliases"`
}

// Default 返回填充好默认值的配置对象。
func Default() Config {
	return Config{
		Listen:   "0.0.0.0:8080",
		APIKeys:  []APIKeyItem{},
		AuthDir:  "./auths",
		DataDir:  "./data",
		LogLevel: "info",
		Retry: RetryConfig{
			MaxRetries:       3,
			BaseBackoffMs:    500,
			MaxBackoffMs:     8000,
			ShortCooldownSec: 60,
			LongCooldownSec:  1800,
		},
		Checkin: CheckinConfig{
			Enabled:     true,
			Time:        "08:30",
			WindowStart: "08:00",
			WindowEnd:   "11:30",
		},
		Routing: RoutingConfig{
			PrefixMode:     "both",
			DisabledModels: []string{},
			Aliases:        map[string][]string{},
		},
	}
}

// Normalize 规范化配置项并补齐合法默认值。
func (c *Config) Normalize() {
	def := Default()
	c.Listen = strings.TrimSpace(c.Listen)
	if c.Listen == "" {
		c.Listen = def.Listen
	}
	c.AuthDir = strings.TrimSpace(c.AuthDir)
	if c.AuthDir == "" {
		c.AuthDir = def.AuthDir
	}
	c.DataDir = strings.TrimSpace(c.DataDir)
	if c.DataDir == "" {
		c.DataDir = def.DataDir
	}
	c.LogLevel = strings.ToLower(strings.TrimSpace(c.LogLevel))
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		c.LogLevel = def.LogLevel
	}
	c.PanelAuth.PasswordHash = strings.TrimSpace(c.PanelAuth.PasswordHash)
	// 签到时刻与随机窗口规范化：窗口非法时回落为固定时刻
	c.Checkin.Time = strings.TrimSpace(c.Checkin.Time)
	if c.Checkin.Time == "" {
		c.Checkin.Time = def.Checkin.Time
	}
	if _, ok := ParseHHMM(c.Checkin.Time); !ok {
		c.Checkin.Time = def.Checkin.Time
	}
	c.Checkin.WindowStart = strings.TrimSpace(c.Checkin.WindowStart)
	c.Checkin.WindowEnd = strings.TrimSpace(c.Checkin.WindowEnd)
	if start, okS := ParseHHMM(c.Checkin.WindowStart); okS {
		if end, okE := ParseHHMM(c.Checkin.WindowEnd); okE && end > start {
			// 合法窗口，保留
		} else {
			c.Checkin.WindowStart, c.Checkin.WindowEnd = "", ""
		}
	} else {
		c.Checkin.WindowStart, c.Checkin.WindowEnd = "", ""
	}

	// 规范化 API Key 列表并按密钥去重
	cleanKeys := make([]APIKeyItem, 0, len(c.APIKeys))
	seenKeys := make(map[string]struct{}, len(c.APIKeys))
	for i, item := range c.APIKeys {
		item.Key = strings.TrimSpace(item.Key)
		if item.Key == "" {
			continue
		}
		if _, ok := seenKeys[item.Key]; ok {
			continue
		}
		seenKeys[item.Key] = struct{}{}
		item.ID = strings.TrimSpace(item.ID)
		if item.ID == "" {
			item.ID = GenerateItemID("key")
		}
		item.Name = strings.TrimSpace(item.Name)
		if item.Name == "" {
			item.Name = fmt.Sprintf("密钥-%d", i+1)
		}
		item.CreatedAt = strings.TrimSpace(item.CreatedAt)
		if item.CreatedAt == "" {
			item.CreatedAt = time.Now().Format("2006-01-02 15:04:05")
		}
		cleanKeys = append(cleanKeys, item)
	}
	c.APIKeys = cleanKeys

	c.Proxy.Default = strings.TrimSpace(c.Proxy.Default)
	c.Proxy.CN = strings.TrimSpace(c.Proxy.CN)
	c.Proxy.Global = strings.TrimSpace(c.Proxy.Global)

	c.Upstream.CNBaseURL = strings.TrimRight(strings.TrimSpace(c.Upstream.CNBaseURL), "/")
	c.Upstream.GlobalBaseURL = strings.TrimRight(strings.TrimSpace(c.Upstream.GlobalBaseURL), "/")

	if c.Retry.MaxRetries < 0 {
		c.Retry.MaxRetries = def.Retry.MaxRetries
	}
	if c.Retry.BaseBackoffMs <= 0 {
		c.Retry.BaseBackoffMs = def.Retry.BaseBackoffMs
	}
	if c.Retry.MaxBackoffMs < c.Retry.BaseBackoffMs {
		c.Retry.MaxBackoffMs = max(def.Retry.MaxBackoffMs, c.Retry.BaseBackoffMs)
	}
	if c.Retry.ShortCooldownSec <= 0 {
		c.Retry.ShortCooldownSec = def.Retry.ShortCooldownSec
	}
	if c.Retry.LongCooldownSec <= 0 {
		c.Retry.LongCooldownSec = def.Retry.LongCooldownSec
	}

	c.Checkin.Time = NormalizeClockTime(c.Checkin.Time, def.Checkin.Time)

	switch strings.ToLower(strings.TrimSpace(c.Routing.PrefixMode)) {
	case "bare", "prefixed", "both":
		c.Routing.PrefixMode = strings.ToLower(strings.TrimSpace(c.Routing.PrefixMode))
	default:
		c.Routing.PrefixMode = def.Routing.PrefixMode
	}

	if c.Routing.DisabledModels == nil {
		c.Routing.DisabledModels = []string{}
	} else {
		cleanDisabled := make([]string, 0, len(c.Routing.DisabledModels))
		seen := make(map[string]struct{}, len(c.Routing.DisabledModels))
		for _, m := range c.Routing.DisabledModels {
			m = strings.TrimSpace(m)
			if m == "" {
				continue
			}
			key := strings.ToLower(m)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			cleanDisabled = append(cleanDisabled, m)
		}
		c.Routing.DisabledModels = cleanDisabled
	}

	cleanAliases := make(map[string][]string, len(c.Routing.Aliases))
	for alias, targets := range c.Routing.Aliases {
		alias = strings.TrimSpace(alias)
		if alias == "" {
			continue
		}
		validTargets := make([]string, 0, len(targets))
		seen := make(map[string]struct{}, len(targets))
		for _, t := range targets {
			t = strings.TrimSpace(t)
			if t == "" {
				continue
			}
			if _, ok := seen[t]; ok {
				continue
			}
			seen[t] = struct{}{}
			validTargets = append(validTargets, t)
		}
		if len(validTargets) > 0 {
			cleanAliases[alias] = validTargets
		}
	}
	c.Routing.Aliases = cleanAliases
}

// NormalizeClockTime 校验并标准化 HH:MM 时间字符串。
func NormalizeClockTime(raw, fallback string) string {
	raw = strings.TrimSpace(raw)
	parts := strings.Split(raw, ":")
	if len(parts) != 2 {
		return fallback
	}
	var h, m int
	if _, err := fmt.Sscanf(raw, "%d:%d", &h, &m); err != nil {
		return fallback
	}
	if h < 0 || h > 23 || m < 0 || m > 59 {
		return fallback
	}
	return fmt.Sprintf("%02d:%02d", h, m)
}

// Clone 深拷贝一份配置，避免外部修改污染内存状态。
func (c Config) Clone() Config {
	out := c
	out.APIKeys = append([]APIKeyItem(nil), c.APIKeys...)
	out.Routing.DisabledModels = append([]string(nil), c.Routing.DisabledModels...)
	out.Routing.Aliases = make(map[string][]string, len(c.Routing.Aliases))
	for k, v := range c.Routing.Aliases {
		out.Routing.Aliases[k] = append([]string(nil), v...)
	}
	return out
}

// ValidateClientAPIKey 校验网关请求携带的 API Key。
// 当系统未配置任何 API Key 时默认放行；一旦创建了至少一个 API Key，则仅允许已启用的匹配密钥通过。
func (c Config) ValidateClientAPIKey(provided string) (authEnabled bool, valid bool) {
	if len(c.APIKeys) == 0 {
		return false, true
	}
	provided = strings.TrimSpace(provided)
	if provided == "" {
		return true, false
	}
	for _, item := range c.APIKeys {
		if !item.Enabled {
			continue
		}
		if subtle.ConstantTimeCompare([]byte(item.Key), []byte(provided)) == 1 {
			return true, true
		}
	}
	return true, false
}

// ListenPort 从监听地址解析出端口字符串（用于展示控制台访问地址）。
func (c Config) ListenPort() string {
	idx := strings.LastIndex(c.Listen, ":")
	if idx < 0 || idx == len(c.Listen)-1 {
		return "8080"
	}
	return c.Listen[idx+1:]
}

// Manager 提供线程安全的配置读写、持久化与变更通知。
type Manager struct {
	mu        sync.RWMutex
	path      string
	cfg       Config
	listeners []func(Config)
}

// Load 从文件加载配置；若文件不存在则按默认配置自动初始化并写盘。
func Load(path string) (*Manager, error) {
	cfg := Default()
	if strings.TrimSpace(path) == "" {
		path = "config.json"
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("读取配置文件失败: %w", err)
		}
		cfg.Normalize()
		mgr := &Manager{path: path, cfg: cfg}
		if errSave := mgr.saveLocked(); errSave != nil {
			return nil, fmt.Errorf("初始化默认配置文件失败: %w", errSave)
		}
		return mgr, nil
	}

	if err := decodeConfigFlexible(raw, &cfg); err != nil {
		return nil, fmt.Errorf("解析配置内容失败: %w", err)
	}
	cfg.Normalize()
	return &Manager{path: path, cfg: cfg}, nil
}

// decodeConfigFlexible 兼容旧版 api_keys 字符串数组与 routing.aliases 单值/数组两种形态。
func decodeConfigFlexible(raw []byte, dst *Config) error {
	type routingWire struct {
		PrefixMode     string                     `json:"prefix_mode"`
		DisabledModels []string                   `json:"disabled_models"`
		Aliases        map[string]json.RawMessage `json:"aliases"`
	}
	type configWire struct {
		Listen    string            `json:"listen"`
		PanelAuth PanelAuth         `json:"panel_auth"`
		APIKeys   []json.RawMessage `json:"api_keys"`
		AuthDir   string            `json:"auth_dir"`
		DataDir   string            `json:"data_dir"`
		LogLevel  string            `json:"log_level"`
		Proxy     ProxyConfig       `json:"proxy"`
		Upstream  UpstreamConfig    `json:"upstream"`
		Retry     RetryConfig       `json:"retry"`
		Checkin   CheckinConfig     `json:"checkin"`
		Routing   *routingWire      `json:"routing"`
	}
	wire := configWire{
		Listen:    dst.Listen,
		PanelAuth: dst.PanelAuth,
		AuthDir:   dst.AuthDir,
		DataDir:   dst.DataDir,
		LogLevel:  dst.LogLevel,
		Proxy:     dst.Proxy,
		Upstream:  dst.Upstream,
		Retry:     dst.Retry,
		Checkin:   dst.Checkin,
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return err
	}
	dst.Listen = wire.Listen
	dst.PanelAuth = wire.PanelAuth
	dst.AuthDir = wire.AuthDir
	dst.DataDir = wire.DataDir
	dst.LogLevel = wire.LogLevel
	dst.Proxy = wire.Proxy
	dst.Upstream = wire.Upstream
	dst.Retry = wire.Retry
	dst.Checkin = wire.Checkin

	if wire.APIKeys != nil {
		parsedKeys := make([]APIKeyItem, 0, len(wire.APIKeys))
		for i, rawItem := range wire.APIKeys {
			var legacyStr string
			if err := json.Unmarshal(rawItem, &legacyStr); err == nil {
				legacyStr = strings.TrimSpace(legacyStr)
				if legacyStr != "" {
					parsedKeys = append(parsedKeys, APIKeyItem{
						ID:        GenerateItemID("key"),
						Name:      fmt.Sprintf("迁移密钥-%d", i+1),
						Key:       legacyStr,
						Enabled:   true,
						CreatedAt: time.Now().Format("2006-01-02 15:04:05"),
					})
				}
				continue
			}
			var item APIKeyItem
			if err := json.Unmarshal(rawItem, &item); err == nil && strings.TrimSpace(item.Key) != "" {
				parsedKeys = append(parsedKeys, item)
			}
		}
		dst.APIKeys = parsedKeys
	}

	if wire.Routing != nil {
		if wire.Routing.PrefixMode != "" {
			dst.Routing.PrefixMode = wire.Routing.PrefixMode
		}
		if wire.Routing.DisabledModels != nil {
			dst.Routing.DisabledModels = wire.Routing.DisabledModels
		}
		if wire.Routing.Aliases != nil {
			parsed := make(map[string][]string, len(wire.Routing.Aliases))
			for alias, val := range wire.Routing.Aliases {
				var single string
				if err := json.Unmarshal(val, &single); err == nil {
					if strings.TrimSpace(single) != "" {
						parsed[alias] = []string{strings.TrimSpace(single)}
					}
					continue
				}
				var list []string
				if err := json.Unmarshal(val, &list); err == nil {
					parsed[alias] = list
				}
			}
			dst.Routing.Aliases = parsed
		}
	}
	return nil
}

// Get 返回当前配置副本。
func (m *Manager) Get() Config {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cfg.Clone()
}

// Path 返回配置文件路径。
func (m *Manager) Path() string {
	return m.path
}

// OnChange 注册配置更新回调。
func (m *Manager) OnChange(fn func(Config)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listeners = append(m.listeners, fn)
}

// Update 原子修改配置并写回磁盘，随后触发所有监听器。
func (m *Manager) Update(mutate func(*Config) error) (Config, error) {
	m.mu.Lock()
	next := m.cfg.Clone()
	if err := mutate(&next); err != nil {
		m.mu.Unlock()
		return Config{}, err
	}
	next.Normalize()
	prev := m.cfg
	m.cfg = next
	if err := m.saveLocked(); err != nil {
		m.cfg = prev
		m.mu.Unlock()
		return Config{}, err
	}
	snap := m.cfg.Clone()
	listeners := append([]func(Config){}, m.listeners...)
	m.mu.Unlock()

	for _, fn := range listeners {
		fn(snap)
	}
	return snap, nil
}

func (m *Manager) saveLocked() error {
	if dir := filepath.Dir(m.path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	data, err := json.MarshalIndent(m.cfg, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	tmp := m.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, m.path)
}
