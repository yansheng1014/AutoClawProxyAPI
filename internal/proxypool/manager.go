package proxypool

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"autoclaw2api/internal/config"
	"autoclaw2api/internal/logging"
	"autoclaw2api/internal/netx"
)

type nodeHealth struct {
	Status    string
	Checking  bool
	LatencyMS int64
	CheckedAt time.Time
	LastError string
}

// Manager 负责管理全局代理池、三级出口绑定（账号 → 区域 → 默认直连）、测活调度与节点专属连接池。
type Manager struct {
	mu           sync.RWMutex
	filePath     string
	logger       *logging.Logger
	state        diskState
	health       map[string]*nodeHealth
	transports   map[string]*http.Transport
	directTr     *http.Transport
	poolRR       uint64
	healthWakeCh chan struct{}
	probeSlots   chan struct{}
}

// NewManager 初始化代理池管理器，加载持久化数据并在首次升级时兼容迁移旧版静态代理配置。
func NewManager(dataDir string, legacyProxy config.ProxyConfig, logger *logging.Logger) (*Manager, error) {
	if strings.TrimSpace(dataDir) == "" {
		dataDir = "./data"
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("创建数据目录失败: %w", err)
	}
	path := filepath.Join(dataDir, "proxypool.json")
	m := &Manager{
		filePath:     path,
		logger:       logger,
		health:       make(map[string]*nodeHealth),
		transports:   make(map[string]*http.Transport),
		directTr:     buildDirectTransport(),
		healthWakeCh: make(chan struct{}, 1),
		probeSlots:   make(chan struct{}, 8),
		state: diskState{
			Nodes:    []Node{},
			Bindings: map[string]Binding{},
			Regions: map[string]Binding{
				"cn":     {Mode: ModeDirect},
				"global": {Mode: ModeDirect},
			},
			Health: DefaultHealthConfig(),
		},
	}

	if err := m.loadOrMigrate(legacyProxy); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Manager) loadOrMigrate(legacy config.ProxyConfig) error {
	raw, err := os.ReadFile(m.filePath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("读取代理池文件失败: %w", err)
		}
		// 首次初始化时如检测到旧版静态代理配置，自动迁移为节点与区域默认绑定
		m.migrateLegacyConfig(legacy)
		return m.saveLocked()
	}
	var st diskState
	if err := json.Unmarshal(raw, &st); err != nil {
		return fmt.Errorf("解析代理池文件失败: %w", err)
	}
	if st.Nodes == nil {
		st.Nodes = []Node{}
	}
	if st.Bindings == nil {
		st.Bindings = map[string]Binding{}
	}
	if st.Regions == nil {
		st.Regions = map[string]Binding{}
	}
	for _, r := range []string{"cn", "global"} {
		if b, ok := st.Regions[r]; ok {
			st.Regions[r] = b.Normalize(false)
		} else {
			st.Regions[r] = Binding{Mode: ModeDirect}
		}
	}
	st.Health = st.Health.Normalize()
	m.state = st
	for _, n := range m.state.Nodes {
		m.health[n.ID] = &nodeHealth{Status: StatusUnknown}
	}
	return nil
}

func (m *Manager) migrateLegacyConfig(legacy config.ProxyConfig) {
	addOrReuse := func(name, rawURL string) string {
		norm, _, _, err := NormalizeProxyURL(rawURL)
		if err != nil {
			return ""
		}
		for _, n := range m.state.Nodes {
			if n.URL == norm {
				return n.ID
			}
		}
		id := newNodeID()
		m.state.Nodes = append(m.state.Nodes, Node{
			ID:      id,
			Name:    name,
			URL:     norm,
			Enabled: true,
		})
		m.health[id] = &nodeHealth{Status: StatusUnknown}
		return id
	}

	defID := ""
	if legacy.Default != "" {
		defID = addOrReuse("默认迁移代理", legacy.Default)
	}
	if legacy.CN != "" {
		if id := addOrReuse("国内区迁移代理", legacy.CN); id != "" {
			m.state.Regions["cn"] = Binding{Mode: ModeFixed, ProxyID: id}
		}
	} else if defID != "" {
		m.state.Regions["cn"] = Binding{Mode: ModeFixed, ProxyID: defID}
	}
	if legacy.Global != "" {
		if id := addOrReuse("国际区迁移代理", legacy.Global); id != "" {
			m.state.Regions["global"] = Binding{Mode: ModeFixed, ProxyID: id}
		}
	} else if defID != "" {
		m.state.Regions["global"] = Binding{Mode: ModeFixed, ProxyID: defID}
	}
}

func (m *Manager) saveLocked() error {
	data, err := json.MarshalIndent(m.state, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	tmp := m.filePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, m.filePath)
}

func newNodeID() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return "px_" + hex.EncodeToString(b)
}

// Start 启动后台定时健康巡检协程。
func (m *Manager) Start(ctx context.Context) {
	go func() {
		// 启动后稍作延迟执行首轮测活
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
			m.ProbeAll(ctx)
		}

		for {
			m.mu.RLock()
			hc := m.state.Health
			m.mu.RUnlock()

			interval := time.Duration(hc.IntervalSec) * time.Second
			if interval < 10*time.Second {
				interval = 60 * time.Second
			}
			timer := time.NewTimer(interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-m.healthWakeCh:
				timer.Stop()
				continue
			case <-timer.C:
				m.mu.RLock()
				enabled := m.state.Health.Enabled
				m.mu.RUnlock()
				if enabled {
					m.ProbeAll(ctx)
				}
			}
		}
	}()
}

// Snapshot 返回代理池节点列表、区域默认出口与测活配置快照。
func (m *Manager) Snapshot() Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	boundAcc := make(map[string]int)
	for _, b := range m.state.Bindings {
		if b.Mode == ModeFixed && b.ProxyID != "" {
			boundAcc[b.ProxyID]++
		}
	}
	boundReg := make(map[string][]string)
	for reg, b := range m.state.Regions {
		if b.Mode == ModeFixed && b.ProxyID != "" {
			boundReg[b.ProxyID] = append(boundReg[b.ProxyID], reg)
		}
	}

	views := make([]NodeView, 0, len(m.state.Nodes))
	for _, n := range m.state.Nodes {
		h := m.health[n.ID]
		st := StatusUnknown
		var lat int64
		var checkedAt, lastErr string
		if !n.Enabled {
			st = StatusDisabled
		} else if h != nil {
			if h.Checking {
				st = StatusChecking
			} else if h.Status != "" {
				st = h.Status
			}
			lat = h.LatencyMS
			if !h.CheckedAt.IsZero() {
				checkedAt = h.CheckedAt.Format("2006-01-02 15:04:05")
			}
			lastErr = h.LastError
		}
		regs := append([]string(nil), boundReg[n.ID]...)
		sort.Strings(regs)
		views = append(views, NodeView{
			ID:           n.ID,
			Name:         n.Name,
			Protocol:     ProtocolOf(n.URL),
			MaskedURL:    MaskProxyURL(n.URL),
			HasPassword:  HasPassword(n.URL),
			Enabled:      n.Enabled,
			Status:       st,
			LatencyMS:    lat,
			CheckedAt:    checkedAt,
			LastError:    lastErr,
			BoundCount:   boundAcc[n.ID],
			BoundRegions: regs,
		})
	}

	regions := make(map[string]Binding, len(m.state.Regions))
	for k, v := range m.state.Regions {
		regions[k] = v
	}
	return Snapshot{
		Nodes:   views,
		Regions: regions,
		Health:  m.state.Health,
	}
}

// ImportBulk 批量导入代理文本（URI / Base64 / Clash YAML），去重并返回逐条结果。
func (m *Manager) ImportBulk(ctx context.Context, text string) (*ImportSummary, error) {
	entries := ParseBulkInput(text)
	summary := &ImportSummary{
		Items: make([]ImportItemResult, 0, len(entries)),
	}
	if len(entries) == 0 {
		return summary, nil
	}

	m.mu.Lock()
	existingURLs := make(map[string]string, len(m.state.Nodes))
	for _, n := range m.state.Nodes {
		existingURLs[n.URL] = n.ID
	}

	var addedIDs []string
	for _, e := range entries {
		if e.Unsupported {
			summary.Unsupported++
			summary.Items = append(summary.Items, ImportItemResult{
				Line:   e.Line,
				Name:   e.Name,
				Status: "unsupported",
				Reason: e.Reason,
			})
			continue
		}
		if e.Normalized == "" {
			summary.Invalid++
			summary.Items = append(summary.Items, ImportItemResult{
				Line:   e.Line,
				Name:   e.Name,
				Status: "invalid",
				Reason: e.Reason,
			})
			continue
		}
		if dupID, exists := existingURLs[e.Normalized]; exists {
			summary.Duplicate++
			summary.Items = append(summary.Items, ImportItemResult{
				Line:   e.Line,
				Name:   e.Name,
				Status: "duplicate",
				ID:     dupID,
				Reason: "该代理地址已存在于代理池中",
			})
			continue
		}
		id := newNodeID()
		name := strings.TrimSpace(e.Name)
		if name == "" {
			name = "节点-" + id[3:]
		}
		m.state.Nodes = append(m.state.Nodes, Node{
			ID:      id,
			Name:    name,
			URL:     e.Normalized,
			Enabled: true,
		})
		m.health[id] = &nodeHealth{Status: StatusUnknown}
		existingURLs[e.Normalized] = id
		addedIDs = append(addedIDs, id)
		summary.Added++
		summary.Items = append(summary.Items, ImportItemResult{
			Line:   e.Line,
			Name:   name,
			Status: "added",
			ID:     id,
		})
	}
	if len(addedIDs) > 0 {
		if err := m.saveLocked(); err != nil {
			m.mu.Unlock()
			return nil, err
		}
	}
	m.mu.Unlock()

	// 对新导入节点后台异步触发一次测活
	if len(addedIDs) > 0 {
		go func(ids []string) {
			probeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			for _, id := range ids {
				_, _ = m.ProbeNode(probeCtx, id)
			}
		}(addedIDs)
	}
	return summary, nil
}

// UpdateNode 修改节点名称、地址或启停状态；变更时自动关闭并清除旧连接池。
func (m *Manager) UpdateNode(id string, name *string, rawURL *string, enabled *bool) (*NodeView, error) {
	m.mu.Lock()
	idx := -1
	for i := range m.state.Nodes {
		if m.state.Nodes[i].ID == id {
			idx = i
			break
		}
	}
	if idx < 0 {
		m.mu.Unlock()
		return nil, ErrNodeNotFound
	}

	cur := m.state.Nodes[idx]
	next := cur
	if name != nil {
		trimmed := strings.TrimSpace(*name)
		if trimmed != "" {
			next.Name = trimmed
		}
	}
	if rawURL != nil && strings.TrimSpace(*rawURL) != "" {
		trimmedURL := strings.TrimSpace(*rawURL)
		// 如果前端传回带 *** 掩码的原地址，则保持原 URL 与原密码不变
		if trimmedURL != MaskProxyURL(cur.URL) {
			norm, _, _, err := NormalizeProxyURL(trimmedURL)
			if err != nil {
				m.mu.Unlock()
				return nil, err
			}
			for j, other := range m.state.Nodes {
				if j != idx && other.URL == norm {
					m.mu.Unlock()
					return nil, errors.New("修改后的代理地址与其他节点重复")
				}
			}
			next.URL = norm
		}
	}
	if enabled != nil {
		next.Enabled = *enabled
	}

	transportChanged := next.URL != cur.URL || next.Enabled != cur.Enabled
	m.state.Nodes[idx] = next
	if err := m.saveLocked(); err != nil {
		m.state.Nodes[idx] = cur
		m.mu.Unlock()
		return nil, err
	}
	if transportChanged {
		m.evictTransportLocked(id)
		if next.URL != cur.URL {
			m.health[id] = &nodeHealth{Status: StatusUnknown}
		}
	}
	m.mu.Unlock()

	snap := m.Snapshot()
	for _, v := range snap.Nodes {
		if v.ID == id {
			cp := v
			return &cp, nil
		}
	}
	return nil, ErrNodeNotFound
}

// DeleteNode 删除指定节点（带防误删保护：存在账号或区域固定绑定时拒绝删除）。
func (m *Manager) DeleteNode(id string) error {
	m.mu.Lock()
	idx := -1
	for i := range m.state.Nodes {
		if m.state.Nodes[i].ID == id {
			idx = i
			break
		}
	}
	if idx < 0 {
		m.mu.Unlock()
		return ErrNodeNotFound
	}

	boundAcc := 0
	for _, b := range m.state.Bindings {
		if b.Mode == ModeFixed && b.ProxyID == id {
			boundAcc++
		}
	}
	var boundRegs []string
	for reg, b := range m.state.Regions {
		if b.Mode == ModeFixed && b.ProxyID == id {
			boundRegs = append(boundRegs, reg)
		}
	}
	if boundAcc > 0 || len(boundRegs) > 0 {
		m.mu.Unlock()
		return fmt.Errorf("%w（当前被 %d 个账号及 %d 个区域默认出口绑定）", ErrNodeBound, boundAcc, len(boundRegs))
	}

	m.state.Nodes = append(m.state.Nodes[:idx], m.state.Nodes[idx+1:]...)
	delete(m.health, id)
	m.evictTransportLocked(id)
	err := m.saveLocked()
	m.mu.Unlock()
	return err
}

// SetRegionBinding 设置国内区（cn）或国际区（global）的默认出口策略。
func (m *Manager) SetRegionBinding(region string, b Binding) error {
	reg := strings.ToLower(strings.TrimSpace(region))
	if reg != "cn" && reg != "global" {
		return fmt.Errorf("无效区域标识: %s（仅支持 cn / global）", region)
	}
	norm := b.Normalize(false)

	m.mu.Lock()
	defer m.mu.Unlock()
	if norm.Mode == ModeFixed {
		if !m.nodeExistsLocked(norm.ProxyID) {
			return ErrNodeNotFound
		}
	}
	m.state.Regions[reg] = norm
	return m.saveLocked()
}

// SetAccountBinding 设置单个账号的出口绑定策略（inherit / direct / pool / fixed）。
func (m *Manager) SetAccountBinding(accountID string, b Binding) error {
	accountID = strings.TrimSpace(accountID)
	if accountID == "" {
		return errors.New("账号 ID 不能为空")
	}
	norm := b.Normalize(true)

	m.mu.Lock()
	defer m.mu.Unlock()
	if norm.Mode == ModeFixed {
		if !m.nodeExistsLocked(norm.ProxyID) {
			return ErrNodeNotFound
		}
	}
	if norm.Mode == ModeInherit {
		delete(m.state.Bindings, accountID)
	} else {
		m.state.Bindings[accountID] = norm
	}
	return m.saveLocked()
}

// RemoveAccountBinding 删除账号时自动清理其绑定记录，避免遗留孤立绑定阻碍节点删除。
func (m *Manager) RemoveAccountBinding(accountID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.state.Bindings[accountID]; ok {
		delete(m.state.Bindings, accountID)
		_ = m.saveLocked()
	}
}

// UpdateHealthConfig 更新后台测活参数并唤醒定时器立即应用。
func (m *Manager) UpdateHealthConfig(hc HealthConfig) (HealthConfig, error) {
	norm := hc.Normalize()
	if u, err := url.Parse(norm.URL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return HealthConfig{}, errors.New("测活目标地址必须是合法的 HTTP 或 HTTPS URL")
	}
	m.mu.Lock()
	m.state.Health = norm
	err := m.saveLocked()
	m.mu.Unlock()
	if err != nil {
		return HealthConfig{}, err
	}
	select {
	case m.healthWakeCh <- struct{}{}:
	default:
	}
	return norm, nil
}

// DescribeAccountRoute 计算某账号当前实际生效的出口信息（用于控制台展示）。
func (m *Manager) DescribeAccountRoute(accountID, region string) RouteInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	reg := strings.ToLower(strings.TrimSpace(region))
	accBind, ok := m.state.Bindings[accountID]
	if !ok {
		accBind = Binding{Mode: ModeInherit}
	}

	eff := accBind
	fromRegion := false
	if eff.Mode == ModeInherit {
		fromRegion = true
		if rb, exists := m.state.Regions[reg]; exists {
			eff = rb
		} else {
			eff = Binding{Mode: ModeDirect}
		}
	}

	info := RouteInfo{
		AccountMode:   accBind.Mode,
		AccountNodeID: accBind.ProxyID,
		EffectiveMode: eff.Mode,
	}

	prefix := ""
	if fromRegion {
		prefix = "继承区域 → "
	}

	switch eff.Mode {
	case ModeDirect:
		info.Summary = prefix + "直连"
	case ModePool:
		// 池模式下按与请求一致的账号哈希解析当前亲和节点，让控制台能看到实际出口
		if node, found := m.pickPoolNodeLocked(accountID); found {
			info.NodeID = node.ID
			info.NodeName = node.Name
			info.NodeMaskedURL = MaskProxyURL(node.URL)
			tag := ""
			if h := m.health[node.ID]; h != nil && h.Status == StatusUnhealthy {
				tag = "（该节点暂不可用，下次请求自动换节点）"
			}
			info.Summary = fmt.Sprintf("%s代理池自动分配 → 当前节点：%s%s", prefix, node.Name, tag)
		} else {
			info.Summary = prefix + "代理池自动分配（暂无可用节点）"
		}
	case ModeFixed:
		if node, found := m.findNodeLocked(eff.ProxyID); found {
			info.NodeID = node.ID
			info.NodeName = node.Name
			info.NodeMaskedURL = MaskProxyURL(node.URL)
			tag := ""
			if !node.Enabled {
				tag = "（已停用）"
			}
			info.Summary = fmt.Sprintf("%s固定节点：%s%s", prefix, node.Name, tag)
		} else {
			info.NodeID = eff.ProxyID
			info.Summary = prefix + "固定节点（节点已丢失）"
		}
	}
	return info
}

// ClientForAccount 根据账号级与区域级三级规则解析专属代理并返回绑定该节点连接池的 *http.Client。
func (m *Manager) ClientForAccount(accountID, region string, timeout time.Duration) (*http.Client, error) {
	tr, err := m.resolveTransport(accountID, region)
	if err != nil {
		return nil, err
	}
	return &http.Client{
		Timeout:   timeout,
		Transport: tr,
	}, nil
}

// ClientForRegion 根据区域默认出口策略返回 *http.Client（用于无账号上下文的匿名登录请求）。
func (m *Manager) ClientForRegion(region string, timeout time.Duration) (*http.Client, error) {
	return m.ClientForAccount("", region, timeout)
}

func (m *Manager) resolveTransport(accountID, region string) (*http.Transport, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	reg := strings.ToLower(strings.TrimSpace(region))
	eff := Binding{Mode: ModeInherit}
	if accountID != "" {
		if b, ok := m.state.Bindings[accountID]; ok {
			eff = b
		}
	}
	if eff.Mode == ModeInherit {
		if rb, ok := m.state.Regions[reg]; ok {
			eff = rb
		} else {
			eff = Binding{Mode: ModeDirect}
		}
	}

	switch eff.Mode {
	case ModeDirect:
		return m.directTr, nil
	case ModeFixed:
		node, ok := m.findNodeLocked(eff.ProxyID)
		if !ok {
			return nil, fmt.Errorf("账号绑定的固定代理节点 (%s) 不存在", eff.ProxyID)
		}
		if !node.Enabled {
			return nil, fmt.Errorf("账号绑定的固定代理节点「%s」已停用", node.Name)
		}
		return m.getOrCreateTransportLocked(node)
	case ModePool:
		node, ok := m.pickPoolNodeLocked(accountID)
		if !ok {
			return nil, errors.New("代理池中暂无可用的已启用节点")
		}
		return m.getOrCreateTransportLocked(node)
	default:
		return m.directTr, nil
	}
}

// fastSubsetLocked 从候选节点中挑出「延迟接近最快」的子集。
// 判定标准：延迟 <= 最快延迟*1.5 + 80ms（未知延迟的节点视为可用，避免刚启动时无节点可选）。
// 返回 nil 表示无法判定（没有有效延迟数据），由调用方沿用原候选集合。
func (m *Manager) fastSubsetLocked(pool []Node) []Node {
	var (
		minLatency int64 = -1
		out              = make([]Node, 0, len(pool))
	)
	for _, n := range pool {
		h := m.health[n.ID]
		if h == nil || h.LatencyMS <= 0 {
			continue
		}
		if minLatency < 0 || h.LatencyMS < minLatency {
			minLatency = h.LatencyMS
		}
	}
	if minLatency < 0 {
		return nil
	}
	threshold := minLatency*3/2 + 80
	for _, n := range pool {
		h := m.health[n.ID]
		if h == nil || h.LatencyMS <= 0 {
			continue
		}
		if h.LatencyMS <= threshold {
			out = append(out, n)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (m *Manager) pickPoolNodeLocked(accountID string) (Node, bool) {
	var healthy []Node
	var fallback []Node
	for _, n := range m.state.Nodes {
		if !n.Enabled {
			continue
		}
		h := m.health[n.ID]
		if h == nil || h.Status == StatusHealthy || h.Status == StatusUnknown || h.Status == "" {
			healthy = append(healthy, n)
		} else {
			fallback = append(fallback, n)
		}
	}
	pool := healthy
	if len(pool) == 0 {
		pool = fallback
	}
	if len(pool) == 0 {
		return Node{}, false
	}
	// 延迟感知：只在「接近最快」的健康节点集合里做亲和映射，
	// 避免账号被稳定哈希钉死到明显更慢的节点上（实测可差 150ms 以上）。
	if fast := m.fastSubsetLocked(pool); len(fast) > 0 {
		pool = fast
	}
	// 对有账号 ID 的请求优先按稳定哈希亲和映射同一健康节点，无账号 ID 时轮询
	if accountID != "" {
		h := fnv.New32a()
		_, _ = h.Write([]byte(accountID))
		return pool[int(h.Sum32())%len(pool)], true
	}
	idx := m.poolRR % uint64(len(pool))
	m.poolRR++
	return pool[idx], true
}

// ProbeNode 对单个节点执行一次连通性与延迟检测。
func (m *Manager) ProbeNode(ctx context.Context, id string) (*NodeView, error) {
	m.mu.Lock()
	node, ok := m.findNodeLocked(id)
	if !ok {
		m.mu.Unlock()
		return nil, ErrNodeNotFound
	}
	if !node.Enabled {
		m.mu.Unlock()
		return nil, errors.New("节点已停用，无需测活")
	}
	hc := m.state.Health
	tr, errTr := m.getOrCreateTransportLocked(node)
	if errTr != nil {
		m.mu.Unlock()
		return nil, errTr
	}
	if m.health[id] == nil {
		m.health[id] = &nodeHealth{}
	}
	m.health[id].Checking = true
	m.mu.Unlock()

	m.probeSlots <- struct{}{}
	defer func() { <-m.probeSlots }()

	probeCtx, cancel := context.WithTimeout(ctx, time.Duration(hc.TimeoutSec)*time.Second)
	defer cancel()

	client := &http.Client{
		Transport: tr,
		Timeout:   time.Duration(hc.TimeoutSec) * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	// 标准 204 测法：明文 HTTP 请求 generate_204，取 TTFB 作为延迟。
	// 连续采样 probeSamples 次取最小值，避免单次抖动把健康节点误判为不可用。
	const probeSamples = 3
	var (
		bestLatency int64 = -1
		lastErr     error
		lastStatus  int
	)
	for i := 0; i < probeSamples; i++ {
		req, errReq := http.NewRequestWithContext(probeCtx, http.MethodGet, hc.URL, nil)
		if errReq != nil {
			m.recordProbeResult(id, false, 0, "构造探测请求失败: "+errReq.Error())
			return m.findNodeView(id)
		}
		// 使用常见客户端 UA，避免暴露探针特征
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
		req.Header.Set("Accept", "*/*")
		req.Header.Set("Connection", "keep-alive")

		start := time.Now()
		resp, errDo := client.Do(req)
		latency := time.Since(start).Milliseconds()
		if errDo != nil {
			lastErr = errDo
			continue
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		lastStatus = resp.StatusCode
		lastErr = nil
		if resp.StatusCode >= 200 && resp.StatusCode < 500 {
			if bestLatency < 0 || latency < bestLatency {
				bestLatency = latency
			}
		}
		if probeCtx.Err() != nil {
			break
		}
	}

	if bestLatency >= 0 {
		m.recordProbeResult(id, true, bestLatency, "")
		return m.findNodeView(id)
	}
	if lastErr != nil {
		m.recordProbeResult(id, false, 0, lastErr.Error())
	} else {
		m.recordProbeResult(id, false, 0, fmt.Sprintf("探测目标返回 HTTP %d", lastStatus))
	}
	return m.findNodeView(id)
}

// ProbeAll 并发对代理池中所有已启用节点执行测活。
func (m *Manager) ProbeAll(ctx context.Context) int {
	m.mu.RLock()
	ids := make([]string, 0, len(m.state.Nodes))
	for _, n := range m.state.Nodes {
		if n.Enabled {
			ids = append(ids, n.ID)
		}
	}
	m.mu.RUnlock()

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()
			_, _ = m.ProbeNode(ctx, nodeID)
		}(id)
	}
	wg.Wait()
	return len(ids)
}

func (m *Manager) recordProbeResult(id string, ok bool, latencyMS int64, errMsg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, exists := m.health[id]
	if !exists {
		h = &nodeHealth{}
		m.health[id] = h
	}
	prevStatus := h.Status
	h.Checking = false
	h.CheckedAt = time.Now()
	h.LatencyMS = latencyMS
	if ok {
		h.Status = StatusHealthy
		h.LastError = ""
	} else {
		h.Status = StatusUnhealthy
		h.LastError = errMsg
	}

	// 状态翻转时打点：事后可直接从日志里看出「某个节点在什么时刻开始不稳」，
	// 用于区分是代理节点的问题还是上游/链路的问题。
	if m.logger != nil && prevStatus != h.Status {
		name := id
		if n, found := m.findNodeLocked(id); found {
			name = n.Name
		}
		switch h.Status {
		case StatusUnhealthy:
			m.logger.Warn("ProxyPool", "节点「%s」测活失败被摘除（延迟 %dms）：%s", name, latencyMS, errMsg)
		case StatusHealthy:
			if prevStatus != "" && prevStatus != StatusUnknown {
				m.logger.Info("ProxyPool", "节点「%s」恢复健康（延迟 %dms）", name, latencyMS)
			}
		}
	}
}

func (m *Manager) findNodeView(id string) (*NodeView, error) {
	snap := m.Snapshot()
	for _, v := range snap.Nodes {
		if v.ID == id {
			cp := v
			return &cp, nil
		}
	}
	return nil, ErrNodeNotFound
}

func (m *Manager) nodeExistsLocked(id string) bool {
	_, ok := m.findNodeLocked(id)
	return ok
}

func (m *Manager) findNodeLocked(id string) (Node, bool) {
	for _, n := range m.state.Nodes {
		if n.ID == id {
			return n, true
		}
	}
	return Node{}, false
}

func (m *Manager) getOrCreateTransportLocked(node Node) (*http.Transport, error) {
	if tr, ok := m.transports[node.ID]; ok {
		return tr, nil
	}
	tr, err := buildProxyTransport(node.URL)
	if err != nil {
		return nil, err
	}
	m.transports[node.ID] = tr
	return tr, nil
}

// evictTransportLocked 立即关闭并移除旧节点连接池，确保配置变更或停用后不再复用旧连接。
func (m *Manager) evictTransportLocked(id string) {
	if tr, ok := m.transports[id]; ok {
		tr.CloseIdleConnections()
		delete(m.transports, id)
	}
}

// HasCachedTransport 供单元测试检查节点连接池是否处于缓存状态。
func (m *Manager) HasCachedTransport(id string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.transports[id]
	return ok
}

func buildDirectTransport() *http.Transport {
	d := &net.Dialer{
		Timeout:   20 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	return &http.Transport{
		DialContext:           d.DialContext,
		Proxy:                 nil,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          128,
		MaxIdleConnsPerHost:   32,
		IdleConnTimeout:       5 * time.Minute,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: 40 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}

func buildProxyTransport(rawURL string) (*http.Transport, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	d := &net.Dialer{
		Timeout:   20 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	tr := &http.Transport{
		DialContext:       d.DialContext,
		// 家庭宽带/中转类节点在长连接上跑 HTTP/2 极易出现 PROTOCOL_ERROR /
		// stream reset（日志里大量 "stream ID x; PROTOCOL_ERROR"），
		// 改用 HTTP/1.1 分块传输，单请求独占连接，对丢包与中间设备更耐受。
		ForceAttemptHTTP2:     false,
		MaxIdleConns:          64,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       5 * time.Minute,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: 40 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
		tr.Proxy = http.ProxyURL(u)
		return tr, nil
	case "socks5", "socks5h":
		sd, err := netx.NewSOCKS5Dialer(u)
		if err != nil {
			return nil, err
		}
		tr.Proxy = nil
		tr.DialContext = sd.DialContext
		return tr, nil
	default:
		return nil, fmt.Errorf("不支持的代理协议: %s", u.Scheme)
	}
}
