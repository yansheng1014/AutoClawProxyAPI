package proxypool

import (
	"errors"
	"strings"
)

// 出口模式常量。
const (
	ModeInherit = "inherit" // 继承所属区域（CN / Global）默认出口
	ModeDirect  = "direct"  // 强制直连
	ModePool    = "pool"    // 从代理池健康节点自动分配
	ModeFixed   = "fixed"   // 固定绑定某一个代理节点（一号一代理）
)

// 节点健康状态常量。
const (
	StatusHealthy   = "healthy"   // 正常
	StatusChecking  = "checking"  // 检测中
	StatusUnhealthy = "unhealthy" // 异常
	StatusDisabled  = "disabled"  // 已停用
	StatusUnknown   = "unknown"   // 未检测
)

var (
	// ErrNodeNotFound 表示请求的代理节点不存在。
	ErrNodeNotFound = errors.New("代理节点不存在")
	// ErrNodeBound 表示节点仍被账号或区域固定绑定，不能直接删除。
	ErrNodeBound = errors.New("该代理节点仍存在绑定关系，请先解绑后再删除")
)

// Node 表示持久化的代理节点定义。
type Node struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	URL     string `json:"url"`
	Enabled bool   `json:"enabled"`
}

// Binding 描述账号级或区域级的出口绑定配置。
type Binding struct {
	Mode    string `json:"mode"`
	ProxyID string `json:"proxy_id,omitempty"`
}

// Normalize 规范化绑定结构。
func (b Binding) Normalize(allowInherit bool) Binding {
	mode := strings.ToLower(strings.TrimSpace(b.Mode))
	switch mode {
	case ModeDirect, ModePool, ModeFixed:
	case ModeInherit:
		if !allowInherit {
			mode = ModeDirect
		}
	default:
		if allowInherit {
			mode = ModeInherit
		} else {
			mode = ModeDirect
		}
	}
	id := strings.TrimSpace(b.ProxyID)
	if mode != ModeFixed {
		id = ""
	}
	return Binding{Mode: mode, ProxyID: id}
}

// HealthConfig 控制后台定时测活参数。
type HealthConfig struct {
	Enabled     bool   `json:"enabled"`
	URL         string `json:"url"`
	IntervalSec int    `json:"interval_sec"`
	TimeoutSec  int    `json:"timeout_sec"`
}

// DefaultHealthConfig 返回默认测活配置。
// 使用明文 HTTP 的 generate_204 作为标准 204 测法：省去 TLS 握手开销，得到的才是节点真实网络延迟。
func DefaultHealthConfig() HealthConfig {
	return HealthConfig{
		Enabled:     true,
		URL:         "http://www.gstatic.com/generate_204",
		IntervalSec: 60,
		TimeoutSec:  10,
	}
}

// Normalize 规范化并修正越界的测活参数。
func (h HealthConfig) Normalize() HealthConfig {
	def := DefaultHealthConfig()
	h.URL = strings.TrimSpace(h.URL)
	if h.URL == "" {
		h.URL = def.URL
	}
	if h.IntervalSec < 10 {
		h.IntervalSec = def.IntervalSec
	}
	if h.IntervalSec > 86400 {
		h.IntervalSec = 86400
	}
	if h.TimeoutSec < 2 {
		h.TimeoutSec = def.TimeoutSec
	}
	if h.TimeoutSec > 120 {
		h.TimeoutSec = 120
	}
	return h
}

// NodeView 是面向 Web 控制台返回的脱敏节点视图（绝不回显明文密码）。
type NodeView struct {
	ID           string   `json:"id"`
	Name         string   `json:"name"`
	Protocol     string   `json:"protocol"`
	MaskedURL    string   `json:"masked_url"`
	HasPassword  bool     `json:"has_password"`
	Enabled      bool     `json:"enabled"`
	Status       string   `json:"status"`
	LatencyMS    int64    `json:"latency_ms"`
	CheckedAt    string   `json:"checked_at,omitempty"`
	LastError    string   `json:"last_error,omitempty"`
	BoundCount   int      `json:"bound_count"`
	BoundRegions []string `json:"bound_regions,omitempty"`
}

// ImportItemResult 反馈批量导入时每一条代理的详细处理结果。
type ImportItemResult struct {
	Line   int    `json:"line"`
	Name   string `json:"name,omitempty"`
	Status string `json:"status"` // added | duplicate | unsupported | invalid
	ID     string `json:"id,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// ImportSummary 汇总一次批量导入的统计与逐条明细。
type ImportSummary struct {
	Added       int                `json:"added"`
	Duplicate   int                `json:"duplicate"`
	Unsupported int                `json:"unsupported"`
	Invalid     int                `json:"invalid"`
	Items       []ImportItemResult `json:"items"`
}

// RouteInfo 描述某个账号最终解析出的出口说明（供账号列表展示与日志追踪）。
type RouteInfo struct {
	AccountMode   string `json:"account_mode"` // inherit | direct | pool | fixed
	AccountNodeID string `json:"account_proxy_id,omitempty"`
	EffectiveMode string `json:"effective_mode"` // direct | pool | fixed
	NodeID        string `json:"node_id,omitempty"`
	NodeName      string `json:"node_name,omitempty"`
	NodeMaskedURL string `json:"node_masked_url,omitempty"`
	Summary       string `json:"summary"`
}

// Snapshot 汇总代理池全部对外状态。
type Snapshot struct {
	Nodes   []NodeView         `json:"nodes"`
	Regions map[string]Binding `json:"regions"`
	Health  HealthConfig       `json:"health"`
}

type diskState struct {
	Nodes    []Node             `json:"nodes"`
	Bindings map[string]Binding `json:"bindings"`
	Regions  map[string]Binding `json:"regions"`
	Health   HealthConfig       `json:"health"`
}
