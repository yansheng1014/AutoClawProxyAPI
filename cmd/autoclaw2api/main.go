package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"autoclaw2api/internal/accounts"
	"autoclaw2api/internal/banner"
	"autoclaw2api/internal/config"
	"autoclaw2api/internal/gateway"
	"autoclaw2api/internal/logging"
	"autoclaw2api/internal/protocol"
	"autoclaw2api/internal/proxypool"
	"autoclaw2api/internal/scheduler"
)

func main() {
	cfgPath := flag.String("config", "config.json", "配置文件路径（不存在时自动生成默认配置）")
	listenOverride := flag.String("listen", "", "临时覆盖监听地址（如 0.0.0.0:8080）")
	flag.Parse()

	cfgMgr, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[Startup] 加载配置失败: %v\n", err)
		os.Exit(1)
	}
	if *listenOverride != "" {
		_, _ = cfgMgr.Update(func(c *config.Config) error {
			c.Listen = *listenOverride
			return nil
		})
	}
	cfg := cfgMgr.Get()

	logger := logging.New(cfg.LogLevel, 600)
	proxyMgr, err := proxypool.NewManager(cfg.DataDir, cfg.Proxy, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[Startup] 初始化代理池失败: %v\n", err)
		os.Exit(1)
	}

	pool, err := accounts.NewPool(cfgMgr, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[Startup] 初始化账号池失败: %v\n", err)
		os.Exit(1)
	}
	router := accounts.NewRouter(cfgMgr, pool, proxyMgr, logger)
	sched := scheduler.New(cfgMgr, pool, router, proxyMgr, logger)
	srv := gateway.NewServer(cfgMgr, pool, router, proxyMgr, sched, logger)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	proxyMgr.Start(ctx)
	sched.Start(ctx)

	httpServer := &http.Server{
		Addr:              cfg.Listen,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 30 * time.Second,
	}

	// 启动后立即打印绿色 ANSI 启动横幅与结构化启动日志
	stats := pool.Stats()
	proxySnap := proxyMgr.Snapshot()
	port := cfg.ListenPort()
	banner.Print(os.Stdout, banner.Info{
		ListenAddr:      cfg.Listen,
		Port:            port,
		ReadyTotal:      stats.ReadyTotal,
		ReadyCN:         stats.ReadyCN,
		ReadyGlobal:     stats.ReadyGlobal,
		CheckinEnabled:  cfg.Checkin.Enabled,
		CheckinTime:     cfg.Checkin.Time,
		CheckinWinStart: cfg.Checkin.WindowStart,
		CheckinWinEnd:   cfg.Checkin.WindowEnd,
		Version:         config.Version,
	})

	logger.Info("Startup", "版本: %s | 配置文件: %s | 凭据目录: %s", config.Version, cfgMgr.Path(), cfg.AuthDir)
	logger.Info("Startup", "凭据池已加载 %d 个账号（国内 %d / 国际 %d，当前就绪 %d 个）",
		stats.Total, stats.CNTotal, stats.GlobalTotal, stats.ReadyTotal)
	logger.Info("Startup", "代理池已加载 %d 个节点（默认出口：国内=%s | 国际=%s）",
		len(proxySnap.Nodes), proxySnap.Regions["cn"].Mode, proxySnap.Regions["global"].Mode)
	logger.Info("Startup", "上游基址: 国内=%s | 国际=%s",
		protocol.ResolveBaseURL(protocol.RegionCN, cfg.Upstream.CNBaseURL),
		protocol.ResolveBaseURL(protocol.RegionGlobal, cfg.Upstream.GlobalBaseURL))
	logger.Info("Startup", "Web 控制台: http://localhost:%s | OpenAI: /v1/chat/completions | Responses: /v1/responses | Anthropic: /v1/messages", port)

	errCh := make(chan error, 1)
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("Startup", "收到退出信号，正在优雅关闭服务…")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = httpServer.Shutdown(shutdownCtx)
	case serveErr := <-errCh:
		logger.Error("Startup", "HTTP 服务监听失败: %v", serveErr)
		os.Exit(1)
	}
}
