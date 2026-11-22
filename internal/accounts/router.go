package accounts

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"autoclaw2api/internal/config"
	"autoclaw2api/internal/logging"
	"autoclaw2api/internal/protocol"
	"autoclaw2api/internal/proxypool"
)

// RouteTarget 表示一条具体的上游路由目标（区域 + 剥离前缀后的裸模型名）。
type RouteTarget struct {
	Region        protocol.Region
	UpstreamModel string
}

// PublicModelItem 表示对外 /v1/models 与控制台展示的模型条目。
type PublicModelItem struct {
	ID              string `json:"id"`
	RawModel        string `json:"raw_model"`
	Region          string `json:"region"`
	Channel         string `json:"channel"`
	Name            string `json:"name"`
	Description     string `json:"description,omitempty"`
	CreditLevel     string `json:"credit_level,omitempty"`
	ContextWindow   int64  `json:"context_window,omitempty"`
	MaxOutputTokens int64  `json:"max_output_tokens,omitempty"`
	IsReasoning     bool   `json:"is_reasoning"`
	SupportsImages  bool   `json:"supports_images"`
	IsAlias         bool   `json:"is_alias"`
	Disabled        bool   `json:"disabled"`
	AliasTargets    string `json:"alias_targets,omitempty"`
}

// RegionSyncStatus 描述单个区域最近一次实时模型同步的状态。
type RegionSyncStatus struct {
	Region       string `json:"region"`
	HasAccount   bool   `json:"has_account"`
	ModelCount   int    `json:"model_count"`
	LastSyncAt   string `json:"last_sync_at,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
}

// Router 维护双区实时同步的模型目录与别名/前缀路由解析（不预置任何静态假模型）。
type Router struct {
	mu       sync.RWMutex
	cfgMgr   *config.Manager
	pool     *Pool
	proxyMgr *proxypool.Manager
	logger   *logging.Logger

	modelsByRegion map[protocol.Region][]protocol.ModelMeta
	lastSyncAt     map[protocol.Region]time.Time
	lastSyncErr    map[protocol.Region]string
	aliasRR        map[string]uint64
}

// NewRouter 创建模型路由器；双区模型目录初始均为空，仅在存在已登录且启用账号时实时向上游同步。
func NewRouter(cfgMgr *config.Manager, pool *Pool, proxyMgr *proxypool.Manager, logger *logging.Logger) *Router {
	return &Router{
		cfgMgr:         cfgMgr,
		pool:           pool,
		proxyMgr:       proxyMgr,
		logger:         logger,
		modelsByRegion: make(map[protocol.Region][]protocol.ModelMeta, 2),
		lastSyncAt:     make(map[protocol.Region]time.Time, 2),
		lastSyncErr:    make(map[protocol.Region]string, 2),
		aliasRR:        make(map[string]uint64),
	}
}

// hasActiveAccount 检查指定区域是否存在至少一个已启用且未失效的账号。
func (r *Router) hasActiveAccount(region protocol.Region) bool {
	for _, acc := range r.pool.AllAccounts() {
		if acc.Cred.Region == region && !acc.Cred.Disabled && !acc.SessionDead {
			return true
		}
	}
	return false
}

// activeAccountsForRegion 返回某区域所有已启用且未失效的账号列表（用于按序尝试模型探测）。
func (r *Router) activeAccountsForRegion(region protocol.Region) []Account {
	all := r.pool.AllAccounts()
	out := make([]Account, 0, len(all))
	for _, acc := range all {
		if acc.Cred.Region == region && !acc.Cred.Disabled && !acc.SessionDead {
			out = append(out, acc)
		}
	}
	return out
}

// SyncRegionModels 实时同步指定区域的模型目录。
// 若该区域不存在已启用且未失效的账号，立刻清空该区域模型目录；若上游探测失败，记录明确原因且绝不回落编造假模型。
func (r *Router) SyncRegionModels(ctx context.Context, region protocol.Region) ([]protocol.ModelMeta, error) {
	candidates := r.activeAccountsForRegion(region)
	if len(candidates) == 0 {
		r.mu.Lock()
		delete(r.modelsByRegion, region)
		delete(r.lastSyncErr, region)
		delete(r.lastSyncAt, region)
		r.mu.Unlock()
		return nil, nil
	}

	cfg := r.cfgMgr.Get()
	override := cfg.Upstream.CNBaseURL
	if region.IsGlobal() {
		override = cfg.Upstream.GlobalBaseURL
	}
	baseURL := protocol.ResolveBaseURL(region, override)

	var lastErr error
	for _, acc := range candidates {
		client, errClient := r.proxyMgr.ClientForAccount(acc.ID, string(region), 30*time.Second)
		if errClient != nil {
			lastErr = fmt.Errorf("账号 [%s] 代理出口不可用: %w", acc.Cred.DisplayLabel(), errClient)
			continue
		}
		models, errFetch := protocol.FetchModelsConfig(ctx, client, baseURL, acc.Cred.AccessToken, region)
		if errFetch == nil {
			r.mu.Lock()
			r.modelsByRegion[region] = models
			r.lastSyncAt[region] = time.Now()
			r.lastSyncErr[region] = ""
			r.mu.Unlock()
			if r.logger != nil {
				r.logger.Info("Router", "已实时同步 %s 模型目录，共发现 %d 个模型", region.DisplayName(), len(models))
			}
			return models, nil
		}
		lastErr = errFetch
		var uerr *protocol.UpstreamError
		if errors.As(errFetch, &uerr) && uerr.Kind == protocol.ErrorKindSessionDead {
			r.pool.ReportError(acc.ID, uerr)
		}
	}

	errMsg := "上游模型目录探测失败"
	if lastErr != nil {
		errMsg = lastErr.Error()
	}
	// 只有该区域确实没有活跃账号时才清空模型缓存；
	// 仍有账号在线时保留上一轮成功同步的缓存，避免一次瞬时网络抖动导致 /v1/models 被清空、
	// 下游误判整条渠道不可用。
	keepCache := r.hasActiveAccount(region)
	r.mu.Lock()
	if !keepCache {
		delete(r.modelsByRegion, region)
	}
	r.lastSyncAt[region] = time.Now()
	r.lastSyncErr[region] = errMsg
	cachedCount := len(r.modelsByRegion[region])
	r.mu.Unlock()

	if r.logger != nil {
		if keepCache {
			r.logger.Warn("Router", "实时同步 %s 模型目录失败，沿用上次缓存（%d 个模型）: %s", region.DisplayName(), cachedCount, errMsg)
		} else {
			r.logger.Warn("Router", "实时同步 %s 模型目录失败（该区域无活跃账号，已清空）: %s", region.DisplayName(), errMsg)
		}
	}
	return nil, errors.New(errMsg)
}

// RefreshAllModels 同步双区（CN 与 Global）模型目录，返回各区同步状态。
func (r *Router) RefreshAllModels(ctx context.Context) map[string]RegionSyncStatus {
	var wg sync.WaitGroup
	for _, reg := range []protocol.Region{protocol.RegionCN, protocol.RegionGlobal} {
		wg.Add(1)
		go func(region protocol.Region) {
			defer wg.Done()
			_, _ = r.SyncRegionModels(ctx, region)
		}(reg)
	}
	wg.Wait()
	return r.SyncStatuses()
}

// PruneInactiveRegions 检查双区账号状态，一旦某区域无有效账号立即清空其模型目录。
func (r *Router) PruneInactiveRegions() {
	for _, reg := range []protocol.Region{protocol.RegionCN, protocol.RegionGlobal} {
		if !r.hasActiveAccount(reg) {
			r.mu.Lock()
			delete(r.modelsByRegion, reg)
			delete(r.lastSyncErr, reg)
			delete(r.lastSyncAt, reg)
			r.mu.Unlock()
		}
	}
}

// SyncStatuses 返回双区最近一次实时模型同步的状态摘要。
func (r *Router) SyncStatuses() map[string]RegionSyncStatus {
	r.PruneInactiveRegions()
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make(map[string]RegionSyncStatus, 2)
	for _, reg := range []protocol.Region{protocol.RegionCN, protocol.RegionGlobal} {
		hasAcc := r.hasActiveAccount(reg)
		models := r.modelsByRegion[reg]
		syncAt := ""
		if t := r.lastSyncAt[reg]; !t.IsZero() {
			syncAt = t.Format("2006-01-02 15:04:05")
		}
		out[string(reg)] = RegionSyncStatus{
			Region:       string(reg),
			HasAccount:   hasAcc,
			ModelCount:   len(models),
			LastSyncAt:   syncAt,
			ErrorMessage: r.lastSyncErr[reg],
		}
	}
	return out
}

// ResolveTargets 将客户端请求的模型名解析为有序的候选路由目标列表（支持别名跨区均衡、渠道前缀定点与裸名自动匹配）。
func (r *Router) ResolveTargets(requested string) ([]RouteTarget, error) {
	modelName := strings.TrimSpace(requested)
	if modelName == "" {
		return nil, fmt.Errorf("请求缺少 model 参数")
	}

	r.PruneInactiveRegions()
	cfg := r.cfgMgr.Get()
	if isModelDisabled(modelName, cfg.Routing.DisabledModels) {
		return nil, fmt.Errorf("模型 %s 已被管理员禁用", modelName)
	}

	// 1. 检查是否命中自定义别名
	if aliasTargets, ok := lookupAlias(modelName, cfg.Routing.Aliases); ok && len(aliasTargets) > 0 {
		expanded := make([]RouteTarget, 0, len(aliasTargets)*2)
		for _, targetSpec := range aliasTargets {
			if isModelDisabled(targetSpec, cfg.Routing.DisabledModels) {
				continue
			}
			expanded = append(expanded, r.resolveSingleSpec(targetSpec)...)
		}
		if len(expanded) == 0 {
			return nil, fmt.Errorf("别名 %s 指向的目标模型均不可用或已禁用", modelName)
		}
		if len(expanded) > 1 {
			r.mu.Lock()
			offset := int(r.aliasRR[strings.ToLower(modelName)] % uint64(len(expanded)))
			r.aliasRR[strings.ToLower(modelName)]++
			r.mu.Unlock()
			rotated := make([]RouteTarget, 0, len(expanded))
			rotated = append(rotated, expanded[offset:]...)
			rotated = append(rotated, expanded[:offset]...)
			return rotated, nil
		}
		return expanded, nil
	}

	targets := r.resolveSingleSpec(modelName)
	if len(targets) == 0 {
		return nil, fmt.Errorf("未找到可用路由目标: %s", modelName)
	}
	return targets, nil
}

func (r *Router) resolveSingleSpec(spec string) []RouteTarget {
	spec = strings.TrimSpace(spec)
	low := strings.ToLower(spec)

	// 渠道前缀定点路由：autoclawcn:<model> / autoclawglobal:<model>
	if strings.HasPrefix(low, protocol.ChannelCN+":") {
		raw := strings.TrimSpace(spec[len(protocol.ChannelCN)+1:])
		if raw == "" {
			return nil
		}
		return []RouteTarget{{Region: protocol.RegionCN, UpstreamModel: raw}}
	}
	if strings.HasPrefix(low, protocol.ChannelGlobal+":") {
		raw := strings.TrimSpace(spec[len(protocol.ChannelGlobal)+1:])
		if raw == "" {
			return nil
		}
		return []RouteTarget{{Region: protocol.RegionGlobal, UpstreamModel: raw}}
	}

	// 裸模型名自动匹配已同步目录中存在该模型的区域
	inCN := r.hasModelInRegion(protocol.RegionCN, spec)
	inGlobal := r.hasModelInRegion(protocol.RegionGlobal, spec)

	stats := r.pool.Stats()
	switch {
	case inCN && inGlobal:
		targets := []RouteTarget{
			{Region: protocol.RegionCN, UpstreamModel: spec},
			{Region: protocol.RegionGlobal, UpstreamModel: spec},
		}
		r.mu.Lock()
		offset := int(r.aliasRR["bare:"+low] % 2)
		r.aliasRR["bare:"+low]++
		r.mu.Unlock()
		if offset == 1 {
			return []RouteTarget{targets[1], targets[0]}
		}
		return targets
	case inCN:
		return []RouteTarget{{Region: protocol.RegionCN, UpstreamModel: spec}}
	case inGlobal:
		return []RouteTarget{{Region: protocol.RegionGlobal, UpstreamModel: spec}}
	default:
		// 未在已同步目录中命中的透传模型：优先调度存在就绪账号的区域
		var targets []RouteTarget
		if stats.ReadyCN > 0 {
			targets = append(targets, RouteTarget{Region: protocol.RegionCN, UpstreamModel: spec})
		}
		if stats.ReadyGlobal > 0 {
			targets = append(targets, RouteTarget{Region: protocol.RegionGlobal, UpstreamModel: spec})
		}
		return targets
	}
}

func (r *Router) hasModelInRegion(region protocol.Region, modelID string) bool {
	if !r.hasActiveAccount(region) {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, m := range r.modelsByRegion[region] {
		if strings.EqualFold(m.ID, modelID) {
			return true
		}
	}
	return false
}

// ListAllModelsForAdmin 返回控制台模型路由管理视图（仅包含存在有效账号且已同步成功的区域模型）。
func (r *Router) ListAllModelsForAdmin() []PublicModelItem {
	r.PruneInactiveRegions()
	cfg := r.cfgMgr.Get()

	r.mu.RLock()
	cnCopy := append([]protocol.ModelMeta(nil), r.modelsByRegion[protocol.RegionCN]...)
	globalCopy := append([]protocol.ModelMeta(nil), r.modelsByRegion[protocol.RegionGlobal]...)
	r.mu.RUnlock()

	items := make([]PublicModelItem, 0, len(cnCopy)+len(globalCopy)+len(cfg.Routing.Aliases))
	appendRegion := func(list []protocol.ModelMeta, region protocol.Region) {
		channel := region.Channel()
		for _, m := range list {
			prefixed := channel + ":" + m.ID
			items = append(items, PublicModelItem{
				ID:              prefixed,
				RawModel:        m.ID,
				Region:          string(region),
				Channel:         channel,
				Name:            m.Name,
				Description:     m.Description,
				CreditLevel:     m.CreditLevel,
				ContextWindow:   m.ContextWindow,
				MaxOutputTokens: m.MaxOutputTokens,
				IsReasoning:     m.IsReasoning,
				SupportsImages:  m.SupportsImages,
				Disabled:        isModelDisabled(m.ID, cfg.Routing.DisabledModels) || isModelDisabled(prefixed, cfg.Routing.DisabledModels),
			})
		}
	}

	if r.hasActiveAccount(protocol.RegionCN) {
		appendRegion(cnCopy, protocol.RegionCN)
	}
	if r.hasActiveAccount(protocol.RegionGlobal) {
		appendRegion(globalCopy, protocol.RegionGlobal)
	}

	aliasKeys := make([]string, 0, len(cfg.Routing.Aliases))
	for k := range cfg.Routing.Aliases {
		aliasKeys = append(aliasKeys, k)
	}
	sort.Strings(aliasKeys)
	for _, alias := range aliasKeys {
		targets := cfg.Routing.Aliases[alias]
		items = append(items, PublicModelItem{
			ID:           alias,
			RawModel:     alias,
			Region:       "alias",
			Channel:      "alias",
			Name:         alias,
			Description:  "自定义负载均衡别名 → " + strings.Join(targets, ", "),
			IsAlias:      true,
			Disabled:     isModelDisabled(alias, cfg.Routing.DisabledModels),
			AliasTargets: strings.Join(targets, ", "),
		})
	}
	return items
}

// ListOpenAIModels 返回对外 GET /v1/models 暴露的启用模型列表。
// 当某区域无有效账号时，绝不暴露该区域的模型。
func (r *Router) ListOpenAIModels() []PublicModelItem {
	r.PruneInactiveRegions()
	cfg := r.cfgMgr.Get()
	mode := cfg.Routing.PrefixMode

	r.mu.RLock()
	cnCopy := append([]protocol.ModelMeta(nil), r.modelsByRegion[protocol.RegionCN]...)
	globalCopy := append([]protocol.ModelMeta(nil), r.modelsByRegion[protocol.RegionGlobal]...)
	r.mu.RUnlock()

	seen := make(map[string]struct{})
	out := make([]PublicModelItem, 0, len(cnCopy)+len(globalCopy)+len(cfg.Routing.Aliases))

	appendIfEnabled := func(item PublicModelItem) {
		key := strings.ToLower(item.ID)
		if _, exists := seen[key]; exists {
			return
		}
		if isModelDisabled(item.ID, cfg.Routing.DisabledModels) || isModelDisabled(item.RawModel, cfg.Routing.DisabledModels) {
			return
		}
		seen[key] = struct{}{}
		out = append(out, item)
	}

	emitList := func(list []protocol.ModelMeta, region protocol.Region) {
		channel := region.Channel()
		for _, m := range list {
			if mode == "bare" || mode == "both" {
				appendIfEnabled(PublicModelItem{
					ID:          m.ID,
					RawModel:    m.ID,
					Region:      string(region),
					Channel:     channel,
					Name:        m.Name,
					CreditLevel: m.CreditLevel,
				})
			}
			if mode == "prefixed" || mode == "both" {
				appendIfEnabled(PublicModelItem{
					ID:          channel + ":" + m.ID,
					RawModel:    m.ID,
					Region:      string(region),
					Channel:     channel,
					Name:        m.Name,
					CreditLevel: m.CreditLevel,
				})
			}
		}
	}

	if r.hasActiveAccount(protocol.RegionCN) {
		emitList(cnCopy, protocol.RegionCN)
	}
	if r.hasActiveAccount(protocol.RegionGlobal) {
		emitList(globalCopy, protocol.RegionGlobal)
	}

	// 仅在至少存在一个活跃账号时暴露指向它们的自定义别名
	if r.hasActiveAccount(protocol.RegionCN) || r.hasActiveAccount(protocol.RegionGlobal) {
		aliasKeys := make([]string, 0, len(cfg.Routing.Aliases))
		for k := range cfg.Routing.Aliases {
			aliasKeys = append(aliasKeys, k)
		}
		sort.Strings(aliasKeys)
		for _, alias := range aliasKeys {
			appendIfEnabled(PublicModelItem{
				ID:       alias,
				RawModel: alias,
				Region:   "alias",
				Channel:  "alias",
				Name:     alias,
				IsAlias:  true,
			})
		}
	}
	return out
}

func isModelDisabled(id string, disabled []string) bool {
	for _, d := range disabled {
		if strings.EqualFold(strings.TrimSpace(d), strings.TrimSpace(id)) {
			return true
		}
	}
	return false
}

func lookupAlias(name string, aliases map[string][]string) ([]string, bool) {
	if v, ok := aliases[name]; ok {
		return v, true
	}
	for k, v := range aliases {
		if strings.EqualFold(k, name) {
			return v, true
		}
	}
	return nil, false
}
