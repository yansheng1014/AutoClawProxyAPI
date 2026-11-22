package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"autoclaw2api/internal/accounts"
	"autoclaw2api/internal/config"
	"autoclaw2api/internal/logging"
	"autoclaw2api/internal/protocol"
	"autoclaw2api/internal/proxypool"
)

const maxCheckinHistory = 500

// CheckinRecord 记录一次账号签到的详细历史。
type CheckinRecord struct {
	Time           string          `json:"time"`
	Date           string          `json:"date"`
	AccountID      string          `json:"account_id"`
	AccountLabel   string          `json:"account_label"`
	Region         protocol.Region `json:"region"`
	Status         string          `json:"status"` // done | already | failed | skipped
	Points         int64           `json:"points"`
	Message        string          `json:"message"`
	ActiveReported bool            `json:"active_reported"`
	Source         string          `json:"source"` // auto | manual
}

// Engine 驱动 Token 自动续期、每日定时签到、余额同步与凭证目录热重载。
type Engine struct {
	cfgMgr   *config.Manager
	pool     *accounts.Pool
	router   *accounts.Router
	proxyMgr *proxypool.Manager
	logger   *logging.Logger

	mu             sync.RWMutex
	history        []CheckinRecord
	lastAutoDay    string
	runningCheckin bool
	// 续期失败退避：账号 ID -> 下次允许重试时间（内存态，重启即清空，保持轻量）
	refreshBackoff map[string]time.Time
	refreshFails   map[string]int
}

// New 创建后台调度引擎并加载持久化的签到历史。
func New(cfgMgr *config.Manager, pool *accounts.Pool, router *accounts.Router, proxyMgr *proxypool.Manager, logger *logging.Logger) *Engine {
	e := &Engine{
		cfgMgr:         cfgMgr,
		pool:           pool,
		router:         router,
		proxyMgr:       proxyMgr,
		logger:         logger,
		history:        make([]CheckinRecord, 0, 64),
		refreshBackoff: make(map[string]time.Time, 8),
		refreshFails:   make(map[string]int, 8),
	}
	e.loadHistory()
	return e
}

func (e *Engine) historyFilePath() string {
	return filepath.Join(e.cfgMgr.Get().DataDir, "checkin_history.json")
}

func (e *Engine) loadHistory() {
	raw, err := os.ReadFile(e.historyFilePath())
	if err != nil {
		return
	}
	var records []CheckinRecord
	if err := json.Unmarshal(raw, &records); err != nil {
		return
	}
	e.mu.Lock()
	e.history = records
	e.mu.Unlock()

	// 回填今日账号签到状态到内存池中，并恢复「今日自动签到已执行」标记，
	// 避免当天重启后在随机签到时刻再次触发全量签到（重复上报会暴露脚本化特征）。
	today := time.Now().Format("2006-01-02")
	autoDone := false
	for _, r := range records {
		if r.Date != today {
			continue
		}
		e.pool.UpdateCheckinState(r.AccountID, r.Date, r.Status, r.Message)
		if r.Source == "auto" && (r.Status == "done" || r.Status == "already") {
			autoDone = true
		}
	}
	if autoDone {
		e.mu.Lock()
		e.lastAutoDay = today
		e.mu.Unlock()
	}
}

func (e *Engine) appendHistory(rec CheckinRecord) {
	e.mu.Lock()
	e.history = append([]CheckinRecord{rec}, e.history...)
	if len(e.history) > maxCheckinHistory {
		e.history = e.history[:maxCheckinHistory]
	}
	snap := append([]CheckinRecord(nil), e.history...)
	e.mu.Unlock()

	dir := filepath.Dir(e.historyFilePath())
	_ = os.MkdirAll(dir, 0o755)
	if data, err := json.MarshalIndent(snap, "", "  "); err == nil {
		tmp := e.historyFilePath() + ".tmp"
		if os.WriteFile(tmp, append(data, '\n'), 0o600) == nil {
			_ = os.Rename(tmp, e.historyFilePath())
		}
	}
}

// History 返回最近 limit 条签到历史（倒序，最新在前）。
func (e *Engine) History(limit int) []CheckinRecord {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if limit <= 0 || limit > len(e.history) {
		limit = len(e.history)
	}
	out := make([]CheckinRecord, limit)
	copy(out, e.history[:limit])
	return out
}

// Start 启动后台定时调度协程。
func (e *Engine) Start(ctx context.Context) {
	// 启动后异步预热：实时同步双区模型目录并拉取一次余额
	go func() {
		warmupCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
		defer cancel()
		_ = e.router.RefreshAllModels(warmupCtx)
		e.RefreshAllBalances(warmupCtx)
	}()

	go func() {
		syncTicker := time.NewTicker(30 * time.Second)
		refreshTicker := time.NewTicker(15 * time.Minute)
		modelTicker := time.NewTicker(30 * time.Minute)
		balanceTicker := time.NewTicker(30 * time.Minute)
		warmTicker := time.NewTicker(45 * time.Second)
		defer syncTicker.Stop()
		defer refreshTicker.Stop()
		defer modelTicker.Stop()
		defer balanceTicker.Stop()
		defer warmTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-syncTicker.C:
				if err := e.pool.SyncFromDisk(); err != nil && e.logger != nil {
					e.logger.Warn("Scheduler", "扫描凭证目录失败: %v", err)
				}
				e.router.PruneInactiveRegions()
				e.maybeRunDailyCheckin(ctx)
			case <-warmTicker.C:
				// 保活上游连接：经代理的连接空闲后会断，重建要付 TCP+TLS+隧道建立
				// 的代价（实测冷启动首包比热连接慢 3~6 倍），这里用极轻量的请求维持热连接。
				e.keepWarmTransports(ctx)
			case <-refreshTicker.C:
				e.refreshExpiringTokens(ctx)
			case <-balanceTicker.C:
				// 后台轻量巡检余额（无可用账号时直接跳过，避免无谓的上游请求）
				if e.pool.Stats().ReadyTotal > 0 {
					balCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
					e.RefreshAllBalances(balCtx)
					cancel()
				}
			case <-modelTicker.C:
				probeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				_ = e.router.RefreshAllModels(probeCtx)
				cancel()
			}
		}
	}()
}

// keepWarmTransports 周期性对每个可用账号的上游基址发起一次极轻量请求，
// 保持「网关 -> 代理节点 -> 上游」整条链路的 TCP/TLS 连接处于热态。
// 该请求不携带任何业务参数、不消耗额度，仅用于避免冷连接带来的首包延迟。
func (e *Engine) keepWarmTransports(ctx context.Context) {
	cfg := e.cfgMgr.Get()
	list := e.pool.AllAccounts()
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	for _, acc := range list {
		if acc.Cred.Disabled || acc.SessionDead {
			continue
		}
		region := protocol.NormalizeRegion(string(acc.Cred.Region))
		override := cfg.Upstream.GlobalBaseURL
		if !region.IsGlobal() {
			override = cfg.Upstream.CNBaseURL
		}
		baseURL := protocol.ResolveBaseURL(region, override)
		if strings.TrimSpace(baseURL) == "" {
			continue
		}
		client, err := e.proxyMgr.ClientForAccount(acc.ID, string(region), 8*time.Second)
		if err != nil {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(url string, accID string) {
			defer wg.Done()
			defer func() { <-sem }()
			reqCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
			if err != nil {
				return
			}
			req.Header.Set("User-Agent", "okhttp/4.12.0")
			resp, err := client.Do(req)
			if err != nil {
				return
			}
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 512))
			_ = resp.Body.Close()
		}(baseURL, acc.ID)
	}
	wg.Wait()
}

// dailyCheckinTarget 计算「当天」的自动签到目标时刻。
//
// 若配置了 checkin.window_start / window_end 时段，则以日期作为确定性随机种子，
// 在该时段内随机取一个时刻（同一天内重启不会重掷，跨天自动换一个时刻），
// 从而避免每天固定 HH:MM:00 触发这种明显的脚本化特征；
// 未配置时段时退化为 checkin.time 的固定时刻。
func (e *Engine) dailyCheckinTarget(now time.Time) (time.Time, bool) {
	cfg := e.cfgMgr.Get()
	if !cfg.Checkin.Enabled {
		return time.Time{}, false
	}
	loc := now.Location()
	day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc)

	if start, end, ok := cfg.Checkin.CheckinWindow(); ok {
		h := fnv.New32a()
		_, _ = h.Write([]byte(day.Format("2006-01-02")))
		_, _ = h.Write([]byte("|autoclaw2api-daily-checkin|"))
		sum := h.Sum32()
		offsetMin := int(sum % uint32(end-start))
		offsetSec := int((sum >> 8) % 60)
		return day.Add(time.Duration(start+offsetMin)*time.Minute + time.Duration(offsetSec)*time.Second), true
	}

	if min, ok := config.ParseHHMM(cfg.Checkin.Time); ok {
		return day.Add(time.Duration(min) * time.Minute), true
	}
	return time.Time{}, false
}

func (e *Engine) maybeRunDailyCheckin(ctx context.Context) {
	cfg := e.cfgMgr.Get()
	if !cfg.Checkin.Enabled {
		return
	}
	now := time.Now()
	target, ok := e.dailyCheckinTarget(now)
	if !ok || now.Before(target) {
		return
	}
	today := now.Format("2006-01-02")

	e.mu.Lock()
	if e.lastAutoDay == today || e.runningCheckin {
		e.mu.Unlock()
		return
	}
	e.lastAutoDay = today
	e.mu.Unlock()

	if e.logger != nil {
		if start, end, hasWin := cfg.Checkin.CheckinWindow(); hasWin {
			e.logger.Info("Checkin", "今日随机签到时刻 %s 已到（时段 %02d:%02d-%02d:%02d），开始执行全量签到",
				target.Format("15:04:05"), start/60, start%60, end/60, end%60)
		} else {
			e.logger.Info("Checkin", "到达每日自动签到时间（%s），开始执行全量签到任务", cfg.Checkin.Time)
		}
	}
	go func() {
		runCtx, cancel := context.WithTimeout(ctx, 15*time.Minute)
		defer cancel()
		e.CheckinAll(runCtx, "auto")
	}()
}

// refreshWindow 依据 token 的真实生命周期决定「提前多久续期」。
//
// 国际版签发的 accessToken 只有 24 小时有效，若沿用固定 3 天阈值会导致
// 每 15 分钟无条件续期一次（单号每天近百次），显著增加风控暴露面；
// 这里改为按 JWT 的 exp-iat 动态计算：
//   - 短效令牌（<= 48h）：剩余 <= max(2h, ttl/6) 时才续期（24h 令牌约每 20 小时续一次）
//   - 长效令牌（国内版 28 天）：沿用提前 72h
func refreshWindow(cred accounts.Credential) time.Duration {
	const longWindow = 72 * time.Hour
	exp := protocol.JWTExpirySeconds(cred.AccessToken)
	if exp <= 0 {
		exp = cred.ExpiresAt
	}
	iat := protocol.JWTIssuedAtSeconds(cred.AccessToken)
	if exp <= 0 || iat <= 0 || exp <= iat {
		return longWindow
	}
	ttl := time.Duration(exp-iat) * time.Second
	if ttl > 48*time.Hour {
		return longWindow
	}
	win := ttl / 6
	if win < 2*time.Hour {
		win = 2 * time.Hour
	}
	return win
}

// refreshBackoffDelay 返回第 n 次连续失败后的退避时长。
func refreshBackoffDelay(fails int) time.Duration {
	switch {
	case fails <= 0:
		return 0
	case fails == 1:
		return 15 * time.Minute
	case fails == 2:
		return 30 * time.Minute
	case fails == 3:
		return time.Hour
	case fails == 4:
		return 2 * time.Hour
	case fails == 5:
		return 4 * time.Hour
	case fails == 6:
		return 8 * time.Hour
	default:
		return 12 * time.Hour
	}
}

func (e *Engine) refreshExpiringTokens(ctx context.Context) {
	now := time.Now()
	for _, acc := range e.pool.AllAccounts() {
		if acc.Cred.Disabled || acc.Cred.RefreshToken == "" {
			continue
		}
		// 失败退避期内直接跳过，避免死循环刷上游
		e.mu.RLock()
		nextAt, hasBackoff := e.refreshBackoff[acc.ID]
		fails := e.refreshFails[acc.ID]
		e.mu.RUnlock()
		if hasBackoff && now.Before(nextAt) {
			continue
		}

		needRefresh := acc.SessionDead
		if !needRefresh && acc.Cred.ExpiresAt > 0 {
			window := refreshWindow(acc.Cred)
			needRefresh = time.Until(time.Unix(acc.Cred.ExpiresAt, 0)) <= window
		}
		if !needRefresh {
			continue
		}

		runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := e.RefreshAccountToken(runCtx, acc.ID)
		cancel()

		e.mu.Lock()
		if err != nil {
			fails++
			e.refreshFails[acc.ID] = fails
			e.refreshBackoff[acc.ID] = now.Add(refreshBackoffDelay(fails))
		} else {
			delete(e.refreshFails, acc.ID)
			delete(e.refreshBackoff, acc.ID)
		}
		e.mu.Unlock()

		if err != nil {
			if e.logger != nil {
				e.logger.Warn("Scheduler", "账号 %s 自动续期失败（第 %d 次，%.0f 分钟后重试）: %v",
					acc.Cred.DisplayLabel(), fails, refreshBackoffDelay(fails).Minutes(), err)
			}
		} else if e.logger != nil {
			e.logger.Info("Scheduler", "账号 %s Token 自动续期成功（有效期窗口 %s）",
				acc.Cred.DisplayLabel(), refreshWindow(acc.Cred).Round(time.Hour))
		}
	}
}

func (e *Engine) baseURLFor(region protocol.Region) string {
	cfg := e.cfgMgr.Get()
	if region.IsGlobal() {
		return protocol.ResolveBaseURL(region, cfg.Upstream.GlobalBaseURL)
	}
	return protocol.ResolveBaseURL(region, cfg.Upstream.CNBaseURL)
}

// RefreshAccountToken 手动或定时刷新单个账号的访问令牌并无损写回。
func (e *Engine) RefreshAccountToken(ctx context.Context, id string) error {
	acc, ok := e.pool.GetByID(id)
	if !ok {
		return fmt.Errorf("账号不存在: %s", id)
	}
	baseURL := e.baseURLFor(acc.Cred.Region)
	client, errClient := e.proxyMgr.ClientForAccount(acc.ID, string(acc.Cred.Region), 30*time.Second)
	if errClient != nil {
		return fmt.Errorf("代理出口解析失败: %w", errClient)
	}

	res, err := protocol.RefreshToken(ctx, client, baseURL, acc.Cred.AccessToken, acc.Cred.RefreshToken, acc.Cred.DeviceID)
	if err != nil {
		var uerr *protocol.UpstreamError
		if errors.As(err, &uerr) {
			e.pool.ReportError(id, uerr)
		}
		return err
	}
	return e.pool.ApplyRefreshedTokens(id, res.AccessToken, res.RefreshToken, res.ExpiresAt)
}

// RefreshAccountBalance 查询并缓存单个账号的积分余额。
func (e *Engine) RefreshAccountBalance(ctx context.Context, id string) (float64, error) {
	acc, ok := e.pool.GetByID(id)
	if !ok {
		return 0, fmt.Errorf("账号不存在: %s", id)
	}
	baseURL := e.baseURLFor(acc.Cred.Region)
	client, errClient := e.proxyMgr.ClientForAccount(acc.ID, string(acc.Cred.Region), 20*time.Second)
	if errClient != nil {
		return 0, fmt.Errorf("代理出口解析失败: %w", errClient)
	}

	bal, err := protocol.FetchWalletBalance(ctx, client, baseURL, acc.Cred.AccessToken)
	if err != nil {
		var uerr *protocol.UpstreamError
		if errors.As(err, &uerr) {
			e.pool.ReportError(id, uerr)
		}
		return 0, err
	}
	e.pool.UpdateBalance(id, bal)
	return bal, nil
}

// RefreshAllBalances 并发查询所有启用账号的积分余额。
func (e *Engine) RefreshAllBalances(ctx context.Context) int {
	list := e.pool.AllAccounts()
	var wg sync.WaitGroup
	// 并发度收敛到 2：同出口 IP 多号并发查询余额容易触发上游风控
	sem := make(chan struct{}, 2)
	var mu sync.Mutex
	okCount := 0

	for _, acc := range list {
		if acc.Cred.Disabled || acc.SessionDead {
			continue
		}
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if _, err := e.RefreshAccountBalance(ctx, id); err == nil {
				mu.Lock()
				okCount++
				mu.Unlock()
			}
		}(acc.ID)
	}
	wg.Wait()
	return okCount
}

// CheckinAccount 对单个账号执行每日签到，签到成功后上报登录活跃并刷新余额。
func (e *Engine) CheckinAccount(ctx context.Context, id, source string) CheckinRecord {
	now := time.Now()
	today := now.Format("2006-01-02")
	nowStr := now.Format("2006-01-02 15:04:05")
	if source == "" {
		source = "manual"
	}

	acc, ok := e.pool.GetByID(id)
	if !ok {
		return CheckinRecord{
			Time:      nowStr,
			Date:      today,
			AccountID: id,
			Status:    "failed",
			Message:   "账号不存在",
			Source:    source,
		}
	}

	rec := CheckinRecord{
		Time:         nowStr,
		Date:         today,
		AccountID:    acc.ID,
		AccountLabel: acc.Cred.DisplayLabel(),
		Region:       acc.Cred.Region,
		Source:       source,
	}

	if acc.Cred.Disabled {
		rec.Status = "skipped"
		rec.Message = "账号已停用，跳过签到"
		return rec
	}

	baseURL := e.baseURLFor(acc.Cred.Region)
	client, errClient := e.proxyMgr.ClientForAccount(acc.ID, string(acc.Cred.Region), 20*time.Second)
	if errClient != nil {
		rec.Status = "failed"
		rec.Message = "代理出口不可用: " + errClient.Error()
		e.pool.UpdateCheckinState(acc.ID, today, rec.Status, rec.Message)
		e.appendHistory(rec)
		return rec
	}

	outcome, err := protocol.DailyCheckin(ctx, client, baseURL, acc.Cred.AccessToken)
	if err != nil {
		var uerr *protocol.UpstreamError
		if errors.As(err, &uerr) {
			e.pool.ReportError(acc.ID, uerr)
		}
		rec.Status = "failed"
		rec.Message = err.Error()
		e.pool.UpdateCheckinState(acc.ID, today, rec.Status, rec.Message)
		e.appendHistory(rec)
		return rec
	}

	if outcome.AlreadyCompleted {
		rec.Status = "already"
		rec.Message = outcome.Message
		// 已签时顺带补一次登录活跃上报，确保当日活跃奖励触发
		if errAct := protocol.ReportLoginActive(ctx, client, baseURL, acc.Cred.AccessToken, acc.Cred.DeviceID); errAct == nil {
			rec.ActiveReported = true
		}
	} else if outcome.Claimed {
		rec.Status = "done"
		rec.Points = outcome.Points
		rec.Message = outcome.Message
		if errAct := protocol.ReportLoginActive(ctx, client, baseURL, acc.Cred.AccessToken, acc.Cred.DeviceID); errAct == nil {
			rec.ActiveReported = true
		}
		// 签到成功后刷新一次余额
		_, _ = e.RefreshAccountBalance(ctx, acc.ID)
	} else {
		rec.Status = "failed"
		rec.Message = outcome.Message
	}

	e.pool.UpdateCheckinState(acc.ID, today, rec.Status, rec.Message)
	e.appendHistory(rec)
	if e.logger != nil {
		e.logger.Info("Checkin", "账号 [%s] 签到结果: %s (%s)", rec.AccountLabel, rec.Status, rec.Message)
	}
	return rec
}

// CheckinAll 对所有启用且未会话失效的账号批量执行每日签到。
func (e *Engine) CheckinAll(ctx context.Context, source string) []CheckinRecord {
	e.mu.Lock()
	if e.runningCheckin {
		e.mu.Unlock()
		return nil
	}
	e.runningCheckin = true
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		e.runningCheckin = false
		e.mu.Unlock()
	}()

	list := e.pool.AllAccounts()
	today := time.Now().Format("2006-01-02")
	// 自动签到：打乱账号顺序并加入随机间隔，避免多账号在同一秒内连续上报；
	// 当日已签到（done/already）的账号直接跳过，避免重启后重复上报。
	candidates := make([]accounts.Account, 0, len(list))
	for _, acc := range list {
		if acc.Cred.Disabled || acc.SessionDead {
			continue
		}
		if source == "auto" && acc.LastCheckin == today &&
			(acc.CheckinStatus == "done" || acc.CheckinStatus == "already") {
			continue
		}
		candidates = append(candidates, acc)
	}
	if source == "auto" && len(candidates) > 1 {
		rand.Shuffle(len(candidates), func(i, j int) {
			candidates[i], candidates[j] = candidates[j], candidates[i]
		})
	}

	results := make([]CheckinRecord, 0, len(candidates))
	for i, acc := range candidates {
		if source == "auto" && i > 0 {
			// 每号之间 20~150 秒随机间隔
			gap := time.Duration(20+rand.Intn(131)) * time.Second
			select {
			case <-ctx.Done():
				return results
			case <-time.After(gap):
			}
		}
		rec := e.CheckinAccount(ctx, acc.ID, source)
		results = append(results, rec)
	}
	return results
}
