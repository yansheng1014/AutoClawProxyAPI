package accounts

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"autoclaw2api/internal/config"
	"autoclaw2api/internal/logging"
	"autoclaw2api/internal/protocol"
	"autoclaw2api/internal/proxypool"
)

// Status 描述账号当前在调度池中的运行状态。
type Status string

const (
	StatusOnline   Status = "online"   // 在线就绪
	StatusCooldown Status = "cooldown" // 限流或积分不足冷却中
	StatusDisabled Status = "disabled" // 已手动停用
	StatusDead     Status = "dead"     // 会话失效（需重新登录）
)

// Account 代表多账号池中的一个受管账号实体（不设数量上限）。
type Account struct {
	ID            string
	FilePath      string
	FileModTime   time.Time
	FileSize      int64
	Cred          Credential
	SessionDead   bool
	CooldownUntil time.Time
	CooldownKind  string
	LastError     string
	Balance       float64
	BalanceKnown  bool
	BalanceAt     time.Time
	LastCheckin   string // YYYY-MM-DD
	CheckinStatus string // done | already | failed | pending
	CheckinMsg    string
	TotalRequests int64
	FailedCount   int64
}

// EffectiveStatus 计算账号在指定时刻的实时状态。
func (a *Account) EffectiveStatus(now time.Time) Status {
	if a.Cred.Disabled {
		return StatusDisabled
	}
	if a.SessionDead {
		return StatusDead
	}
	if !a.CooldownUntil.IsZero() && now.Before(a.CooldownUntil) {
		return StatusCooldown
	}
	return StatusOnline
}

// AccountView 是面向 Web 控制台与状态查询的脱敏视图结构。
type AccountView struct {
	ID              string               `json:"id"`
	FileName        string               `json:"file_name"`
	Label           string               `json:"label"`
	Alias           string               `json:"alias"`
	UserName        string               `json:"user_name"`
	UserID          string               `json:"user_id"`
	Phone           string               `json:"phone"`
	Note            string               `json:"note"`
	Region          protocol.Region      `json:"region"`
	Channel         string               `json:"channel"`
	Status          Status               `json:"status"`
	Disabled        bool                 `json:"disabled"`
	SessionDead     bool                 `json:"session_dead"`
	CooldownRemainS int64                `json:"cooldown_remain_sec"`
	CooldownKind    string               `json:"cooldown_kind,omitempty"`
	LastError       string               `json:"last_error,omitempty"`
	ExpiresAt       int64                `json:"expires_at"`
	TokenMasked     string               `json:"token_masked"`
	Balance         float64              `json:"balance"`
	BalanceKnown    bool                 `json:"balance_known"`
	BalanceAt       string               `json:"balance_at,omitempty"`
	CheckedInToday  bool                 `json:"checked_in_today"`
	CheckinStatus   string               `json:"checkin_status"`
	CheckinMsg      string               `json:"checkin_msg,omitempty"`
	TotalRequests   int64                `json:"total_requests"`
	FailedCount     int64                `json:"failed_count"`
	ProxyRoute      *proxypool.RouteInfo `json:"proxy_route,omitempty"`
}

// PoolStats 汇总账号池各状态和区域的统计指标。
type PoolStats struct {
	Total        int     `json:"total"`
	CNTotal      int     `json:"cn_total"`
	GlobalTotal  int     `json:"global_total"`
	ReadyTotal   int     `json:"ready_total"`
	ReadyCN      int     `json:"ready_cn"`
	ReadyGlobal  int     `json:"ready_global"`
	Cooldown     int     `json:"cooldown"`
	Disabled     int     `json:"disabled"`
	Dead         int     `json:"dead"`
	TotalBalance float64 `json:"total_balance"`
}

// Pool 管理任意数量的国内版与国际版账号，支持目录热更新、轮询调度与故障冷却。
type Pool struct {
	mu      sync.RWMutex
	cfgMgr  *config.Manager
	logger  *logging.Logger
	items   map[string]*Account
	order   []string
	rrIndex map[string]uint64
}

// NewPool 创建多账号池并立刻从凭证目录完成首次扫描加载。
func NewPool(cfgMgr *config.Manager, logger *logging.Logger) (*Pool, error) {
	p := &Pool{
		cfgMgr:  cfgMgr,
		logger:  logger,
		items:   make(map[string]*Account),
		rrIndex: make(map[string]uint64),
	}
	if err := p.SyncFromDisk(); err != nil {
		return nil, err
	}
	return p, nil
}

// AuthDir 返回当前配置的凭证目录路径。
func (p *Pool) AuthDir() string {
	return p.cfgMgr.Get().AuthDir
}

// SyncFromDisk 扫描凭证目录，热加载新增/修改的凭证文件，并清理已被外部删除的条目。
func (p *Pool) SyncFromDisk() error {
	dir := p.AuthDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("创建凭证目录失败: %w", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("扫描凭证目录失败: %w", err)
	}

	found := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !IsSupportedAuthFileName(name) {
			continue
		}
		fullPath := filepath.Join(dir, name)
		info, errInfo := entry.Info()
		if errInfo != nil {
			continue
		}
		id := strings.TrimSuffix(name, filepath.Ext(name))
		found[id] = struct{}{}

		p.mu.RLock()
		existing, exists := p.items[id]
		unchanged := exists && existing.FileModTime.Equal(info.ModTime()) && existing.FileSize == info.Size()
		p.mu.RUnlock()
		if unchanged {
			continue
		}

		raw, errRead := os.ReadFile(fullPath)
		if errRead != nil {
			if p.logger != nil {
				p.logger.Warn("Accounts", "读取凭证文件 %s 失败: %v", name, errRead)
			}
			continue
		}
		cred, errParse := ParseCredentialJSON(raw, name)
		if errParse != nil {
			if p.logger != nil {
				p.logger.Warn("Accounts", "跳过无效凭证文件 %s: %v", name, errParse)
			}
			continue
		}

		p.mu.Lock()
		if cur, ok := p.items[id]; ok {
			tokenChanged := cur.Cred.AccessToken != cred.AccessToken
			cur.Cred = *cred
			cur.FilePath = fullPath
			cur.FileModTime = info.ModTime()
			cur.FileSize = info.Size()
			if tokenChanged {
				// 凭证更新为新 token 后自动解除会话失效和旧冷却
				cur.SessionDead = false
				cur.CooldownUntil = time.Time{}
				cur.LastError = ""
			}
		} else {
			p.items[id] = &Account{
				ID:          id,
				FilePath:    fullPath,
				FileModTime: info.ModTime(),
				FileSize:    info.Size(),
				Cred:        *cred,
			}
		}
		p.mu.Unlock()
	}

	p.mu.Lock()
	for id := range p.items {
		if _, ok := found[id]; !ok {
			delete(p.items, id)
		}
	}
	p.rebuildOrderLocked()
	p.mu.Unlock()
	return nil
}

func (p *Pool) rebuildOrderLocked() {
	order := make([]string, 0, len(p.items))
	for id := range p.items {
		order = append(order, id)
	}
	sort.Strings(order)
	p.order = order
}

// Acquire 按轮询策略选择一个可用的就绪账号（region 为空表示双区联合调度，exclude 用于单请求故障切号避让）。
// AcquireExact 尝试取用指定账号（用于会话粘性命中）。
// 账号需存在、未被排除、区域匹配且在线，否则返回 false，由调用方回落到常规轮询。
func (p *Pool) AcquireExact(id string, region protocol.Region, exclude map[string]bool) (*Account, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if exclude != nil && exclude[id] {
		return nil, false
	}
	acc, ok := p.items[id]
	if !ok {
		return nil, false
	}
	if region != "" && acc.Cred.Region != region {
		return nil, false
	}
	if acc.EffectiveStatus(time.Now()) != StatusOnline {
		return nil, false
	}
	acc.TotalRequests++
	snap := *acc
	return &snap, true
}

func (p *Pool) Acquire(region protocol.Region, exclude map[string]bool) (*Account, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	candidates := make([]*Account, 0, len(p.order))
	for _, id := range p.order {
		if exclude != nil && exclude[id] {
			continue
		}
		acc := p.items[id]
		if region != "" && acc.Cred.Region != region {
			continue
		}
		if acc.EffectiveStatus(now) != StatusOnline {
			continue
		}
		candidates = append(candidates, acc)
	}
	if len(candidates) == 0 {
		return nil, false
	}

	rrKey := string(region)
	if rrKey == "" {
		rrKey = "all"
	}
	idx := p.rrIndex[rrKey] % uint64(len(candidates))
	p.rrIndex[rrKey]++

	chosen := candidates[idx]
	chosen.TotalRequests++
	snap := *chosen
	return &snap, true
}

// ReportSuccess 标记账号本次调用成功，清理残余错误状态。
func (p *Pool) ReportSuccess(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if acc, ok := p.items[id]; ok {
		acc.LastError = ""
		acc.CooldownUntil = time.Time{}
		acc.CooldownKind = ""
	}
}

// ReportError 根据错误分级更新账号冷却或失效状态。
func (p *Pool) ReportError(id string, uerr *protocol.UpstreamError) {
	if uerr == nil {
		return
	}
	retryCfg := p.cfgMgr.Get().Retry
	p.mu.Lock()
	defer p.mu.Unlock()
	acc, ok := p.items[id]
	if !ok {
		return
	}
	acc.FailedCount++
	acc.LastError = uerr.Message

	now := time.Now()
	switch uerr.Kind {
	case protocol.ErrorKindSessionDead:
		acc.SessionDead = true
		acc.CooldownUntil = time.Time{}
		acc.CooldownKind = "session_dead"
	case protocol.ErrorKindHardCredit:
		dur := time.Duration(retryCfg.LongCooldownSec) * time.Second
		if uerr.RetryAfter > dur {
			dur = uerr.RetryAfter
		}
		acc.CooldownUntil = now.Add(dur)
		acc.CooldownKind = "hard_credit"
	case protocol.ErrorKindSoftRate:
		dur := time.Duration(retryCfg.ShortCooldownSec) * time.Second
		if uerr.RetryAfter > 0 {
			dur = uerr.RetryAfter
		}
		acc.CooldownUntil = now.Add(dur)
		acc.CooldownKind = "soft_rate"
	}
}

// GetByID 获取指定账号的快照副本。
func (p *Pool) GetByID(id string) (*Account, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	acc, ok := p.items[id]
	if !ok {
		return nil, false
	}
	cp := *acc
	return &cp, true
}

// AllAccounts 返回所有账号的快照列表（用于后台任务遍历）。
func (p *Pool) AllAccounts() []Account {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]Account, 0, len(p.order))
	for _, id := range p.order {
		out = append(out, *p.items[id])
	}
	return out
}

// ListViews 返回所有账号的脱敏视图列表。
func (p *Pool) ListViews() []AccountView {
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	today := now.Format("2006-01-02")
	views := make([]AccountView, 0, len(p.order))
	for _, id := range p.order {
		acc := p.items[id]
		st := acc.EffectiveStatus(now)
		var coolRemain int64
		if st == StatusCooldown {
			coolRemain = int64(time.Until(acc.CooldownUntil).Seconds())
			if coolRemain < 1 {
				coolRemain = 1
			}
		}
		balAt := ""
		if !acc.BalanceAt.IsZero() {
			balAt = acc.BalanceAt.Format("2006-01-02 15:04:05")
		}
		views = append(views, AccountView{
			ID:              acc.ID,
			FileName:        filepath.Base(acc.FilePath),
			Label:           acc.Cred.DisplayLabel(),
			Alias:           acc.Cred.Alias,
			UserName:        acc.Cred.UserName,
			UserID:          acc.Cred.UserID,
			Phone:           RedactPhoneNumber(acc.Cred.Phone),
			Note:            acc.Cred.Note,
			Region:          acc.Cred.Region,
			Channel:         acc.Cred.Region.Channel(),
			Status:          st,
			Disabled:        acc.Cred.Disabled,
			SessionDead:     acc.SessionDead,
			CooldownRemainS: coolRemain,
			CooldownKind:    acc.CooldownKind,
			LastError:       acc.LastError,
			ExpiresAt:       acc.Cred.ExpiresAt,
			TokenMasked:     MaskToken(acc.Cred.AccessToken),
			Balance:         acc.Balance,
			BalanceKnown:    acc.BalanceKnown,
			BalanceAt:       balAt,
			CheckedInToday:  acc.LastCheckin == today && (acc.CheckinStatus == "done" || acc.CheckinStatus == "already"),
			CheckinStatus:   acc.CheckinStatus,
			CheckinMsg:      acc.CheckinMsg,
			TotalRequests:   acc.TotalRequests,
			FailedCount:     acc.FailedCount,
		})
	}
	return views
}

// Stats 汇总账号池统计信息。
func (p *Pool) Stats() PoolStats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	var s PoolStats
	for _, acc := range p.items {
		s.Total++
		if acc.Cred.Region.IsGlobal() {
			s.GlobalTotal++
		} else {
			s.CNTotal++
		}
		if acc.BalanceKnown {
			s.TotalBalance += acc.Balance
		}
		switch acc.EffectiveStatus(now) {
		case StatusOnline:
			s.ReadyTotal++
			if acc.Cred.Region.IsGlobal() {
				s.ReadyGlobal++
			} else {
				s.ReadyCN++
			}
		case StatusCooldown:
			s.Cooldown++
		case StatusDisabled:
			s.Disabled++
		case StatusDead:
			s.Dead++
		}
	}
	return s
}

// SaveCredential 将登录获得或导入的凭证写入磁盘并立即加入账号池。
func (p *Pool) SaveCredential(cred *Credential, originalRaw []byte) (*AccountView, error) {
	if cred == nil || strings.TrimSpace(cred.AccessToken) == "" {
		return nil, ErrMissingAccessToken
	}
	dir := p.AuthDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("创建凭证目录失败: %w", err)
	}
	fileName := BuildCredentialFileName(cred.Region, cred.UserID, cred.AccessToken)
	fullPath := filepath.Join(dir, fileName)

	var content []byte
	var err error
	if len(originalRaw) > 0 {
		// 导入 JSON 时保留原始自定义字段，同时补齐标准字段
		content, err = MergeTokensLossless(originalRaw, cred.AccessToken, cred.RefreshToken, cred.ExpiresAt)
		if err != nil {
			content, err = cred.ToStorageJSON()
		}
	} else {
		content, err = cred.ToStorageJSON()
	}
	if err != nil {
		return nil, err
	}
	if err := writeFileAtomic(fullPath, content); err != nil {
		return nil, fmt.Errorf("写入凭证文件失败: %w", err)
	}
	if err := p.SyncFromDisk(); err != nil {
		return nil, err
	}
	id := strings.TrimSuffix(fileName, filepath.Ext(fileName))
	for _, v := range p.ListViews() {
		if v.ID == id {
			cp := v
			return &cp, nil
		}
	}
	return nil, fmt.Errorf("凭证已写入但未能加载账号 %s", id)
}

// ApplyRefreshedTokens 将续期后的新令牌无损合并回原文件并刷新内存状态。
func (p *Pool) ApplyRefreshedTokens(id, accessToken, refreshToken string, expiresAt int64) error {
	p.mu.Lock()
	acc, ok := p.items[id]
	if !ok {
		p.mu.Unlock()
		return fmt.Errorf("账号不存在: %s", id)
	}
	filePath := acc.FilePath
	if refreshToken == "" {
		refreshToken = acc.Cred.RefreshToken
	}
	p.mu.Unlock()

	orig, _ := os.ReadFile(filePath)
	merged, err := MergeTokensLossless(orig, accessToken, refreshToken, expiresAt)
	if err != nil {
		return err
	}
	if err := writeFileAtomic(filePath, merged); err != nil {
		return err
	}
	info, _ := os.Stat(filePath)

	p.mu.Lock()
	if cur, exists := p.items[id]; exists {
		cur.Cred.AccessToken = accessToken
		cur.Cred.RefreshToken = refreshToken
		if expiresAt > 0 {
			cur.Cred.ExpiresAt = expiresAt
		}
		cur.SessionDead = false
		cur.CooldownUntil = time.Time{}
		cur.LastError = ""
		if info != nil {
			cur.FileModTime = info.ModTime()
			cur.FileSize = info.Size()
		}
	}
	p.mu.Unlock()
	return nil
}

// UpdateBalance 更新内存中的账号积分余额缓存。
func (p *Pool) UpdateBalance(id string, balance float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if acc, ok := p.items[id]; ok {
		acc.Balance = balance
		acc.BalanceKnown = true
		acc.BalanceAt = time.Now()
	}
}

// UpdateCheckinState 记录账号今日签到状态。
func (p *Pool) UpdateCheckinState(id, dateStr, status, msg string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if acc, ok := p.items[id]; ok {
		acc.LastCheckin = dateStr
		acc.CheckinStatus = status
		acc.CheckinMsg = msg
	}
}

// UpdateMeta 修改账号别名、备注或启停状态，并无损写回原凭证文件。
func (p *Pool) UpdateMeta(id string, alias, note *string, disabled *bool) error {
	p.mu.Lock()
	acc, ok := p.items[id]
	if !ok {
		p.mu.Unlock()
		return fmt.Errorf("账号不存在: %s", id)
	}
	filePath := acc.FilePath
	p.mu.Unlock()

	patch := map[string]any{}
	if alias != nil {
		patch["alias"] = strings.TrimSpace(*alias)
	}
	if note != nil {
		patch["note"] = strings.TrimSpace(*note)
	}
	if disabled != nil {
		patch["disabled"] = *disabled
	}
	if err := PatchFieldsLossless(filePath, patch); err != nil {
		return err
	}
	info, _ := os.Stat(filePath)

	p.mu.Lock()
	if cur, exists := p.items[id]; exists {
		if alias != nil {
			cur.Cred.Alias = strings.TrimSpace(*alias)
		}
		if note != nil {
			cur.Cred.Note = strings.TrimSpace(*note)
		}
		if disabled != nil {
			cur.Cred.Disabled = *disabled
			if !*disabled {
				// 手动重新启用时解除冷却与失效标记便于立刻复测
				cur.CooldownUntil = time.Time{}
				cur.SessionDead = false
				cur.LastError = ""
			}
		}
		if info != nil {
			cur.FileModTime = info.ModTime()
			cur.FileSize = info.Size()
		}
	}
	p.mu.Unlock()
	return nil
}

// Delete 移除单个账号及其磁盘凭证文件。
func (p *Pool) Delete(id string) error {
	p.mu.Lock()
	acc, ok := p.items[id]
	if !ok {
		p.mu.Unlock()
		return fmt.Errorf("账号不存在: %s", id)
	}
	filePath := acc.FilePath
	delete(p.items, id)
	p.rebuildOrderLocked()
	p.mu.Unlock()

	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// BatchSetDisabled 批量启用或停用多个账号。
func (p *Pool) BatchSetDisabled(ids []string, disabled bool) int {
	count := 0
	for _, id := range ids {
		if err := p.UpdateMeta(id, nil, nil, &disabled); err == nil {
			count++
		}
	}
	return count
}

// BatchDelete 批量删除多个账号。
func (p *Pool) BatchDelete(ids []string) int {
	count := 0
	for _, id := range ids {
		if err := p.Delete(id); err == nil {
			count++
		}
	}
	return count
}
