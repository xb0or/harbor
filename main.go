// Harbor AI 网关：排队 + 错误收敛的轻量 AI Gateway。
//
// 用法:
//
//	harbor --config harbor.yaml
//
// 一个二进制 + 一个配置文件即可运行（计划书 §14）。
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"harbor/internal/config"
	"harbor/internal/db"
	"harbor/internal/executor"
	"harbor/internal/key"
	"harbor/internal/provider"
	"harbor/internal/queue"
	"harbor/internal/server"
	"harbor/internal/stats"
)

func main() {
	cfgPath := flag.String("config", "harbor.yaml", "path to config file")
	flag.Parse()

	// 配置来源优先级：HARBOR_CONFIG 环境变量（完整 YAML 文本，PaaS 部署用）> 文件。
	var cfg *config.Config
	var err error
	if raw := os.Getenv("HARBOR_CONFIG"); raw != "" {
		cfg, err = config.Parse([]byte(raw))
	} else {
		cfg, err = config.Load(*cfgPath)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	logger := newLogger(cfg)

	keyStore := key.NewStore()
	var mgr *provider.Manager

	// --- P2 持久化：SQLite 是运行时权威；yaml 只给空域做种子。db_path 为空 = 纯内存，始终用 yaml。 ---
	var store *db.DB
	if cfg.Storage.DBPath != "" {
		store, err = db.Open(cfg.Storage.DBPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "storage error: %v\n", err)
			os.Exit(1)
		}
		defer store.Close()
		if err := store.Migrate(); err != nil {
			fmt.Fprintf(os.Stderr, "storage error: %v\n", err)
			os.Exit(1)
		}
		// 管理台改过的运行时设置（如心跳保活）优先于 yaml。
		server.ApplyStoredSettings(store, cfg, logger)
		encKey, err := db.LoadOrCreateKey(cfg.Storage.KeyPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "storage error: %v\n", err)
			os.Exit(1)
		}
		logger.Info("persistence enabled", "db_path", cfg.Storage.DBPath, "key_path", orDefault(cfg.Storage.KeyPath, "harbor.key"))

		mgr = provider.NewManager(nil, nil)
		if err := server.RestoreStartup(store, keyStore, mgr,
			cfg.Limits.RequestTimeoutDuration(), cfg.Limits.ProviderCooldownDuration(), encKey, logger); err != nil {
			fmt.Fprintf(os.Stderr, "storage restore error: %v\n", err)
			os.Exit(1)
		}
		if err := server.SeedEmptyFromConfig(store, cfg, keyStore, mgr,
			cfg.Limits.RequestTimeoutDuration(), cfg.Limits.ProviderCooldownDuration(), encKey, logger); err != nil {
			fmt.Fprintf(os.Stderr, "storage seed error: %v\n", err)
			os.Exit(1)
		}
	} else {
		mgr = server.LoadYAMLRuntime(cfg, keyStore, logger)
	}

	// --- Executor（failover + retry budget） ---
	exec := executor.New(mgr, cfg.Limits.MaxProviderAttempts, cfg.Limits.BackoffBaseDuration(), cfg.Limits.ProviderCooldownDuration())

	// --- Scheduler（FIFO + 事件驱动） ---
	sched := queue.NewScheduler(cfg.Queue.MaxDepth, cfg.Queue.MaxWaitDuration())
	sched.Start()
	defer sched.Stop()

	// --- HTTP Server ---
	st := stats.New()
	srv := server.New(cfg, keyStore, mgr, exec, sched, st, store, logger)

	httpServer := &http.Server{
		Addr:              cfg.Server.Listen,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 30 * time.Second,
		ReadTimeout:       cfg.Server.ReadTimeoutDuration(),
		IdleTimeout:       cfg.Server.IdleTimeoutDuration(),
		MaxHeaderBytes:    cfg.Server.MaxHeaderBytes,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("harbor listening", "addr", cfg.Server.Listen)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	// 优雅关闭：SIGINT / SIGTERM
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-errCh:
		logger.Error("server error", "err", err)
		os.Exit(1)
	case s := <-sig:
		logger.Info("shutting down", "signal", s.String(), "drain_timeout", "60s")
		// 部署/重启时先关闭监听，让排队与流式请求在窗口内收尾；
		// 超过窗口的请求由 Scheduler.Stop 收敛为 503 server_restarting（可重试）。
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(ctx); err != nil {
			logger.Warn("graceful shutdown timed out", "err", err)
			// 超时后仍给剩余 handler 一点时间把 503 写回客户端，避免连接被直接切断。
			time.Sleep(2 * time.Second)
		}
		logger.Info("shutdown complete")
	}
}

func newLogger(cfg *config.Config) *slog.Logger {
	level := slog.LevelInfo
	switch cfg.Log.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	opts := &slog.HandlerOptions{Level: level}
	if cfg.Log.File != "" {
		f, err := os.OpenFile(cfg.Log.File, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			fmt.Fprintf(os.Stderr, "open log file: %v\n", err)
			os.Exit(1)
		}
		return slog.New(slog.NewJSONHandler(f, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stdout, opts))
}

// orDefault 返回 v；为空时返回 def（日志展示用）。
func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
