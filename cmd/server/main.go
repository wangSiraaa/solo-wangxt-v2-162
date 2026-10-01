// 纠删码对象存储教学项目入口。
//
// 用本地多个目录模拟独立存储节点（STORAGE_ROOT/node0、node1 ...），
// klauspost/reedsolomon 提供 Reed-Solomon 纠删码，
// PostgreSQL 保存对象清单、分片布局与逐块校验值。
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"erasure-store/internal/api"
	"erasure-store/internal/config"
	"erasure-store/internal/db"
	"erasure-store/internal/store"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("配置错误: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("数据库初始化失败: %v", err)
	}
	defer pool.Close()

	mgr, err := store.NewManager(pool, cfg.StorageRoot, cfg.DataShards, cfg.ParityShards)
	if err != nil {
		log.Fatalf("存储管理器初始化失败: %v", err)
	}
	if err := mgr.EnsureNodes(ctx, cfg.DataShards+cfg.ParityShards); err != nil {
		log.Fatalf("节点初始化失败: %v", err)
	}

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.NewServer(mgr, cfg.MaxObjectBytes).Routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("纠删码存储服务启动: %s（数据分片=%d 校验分片=%d，节点根目录=%s）",
			cfg.HTTPAddr, cfg.DataShards, cfg.ParityShards, cfg.StorageRoot)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP 服务退出: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("收到退出信号，正在关闭服务……")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("优雅关闭失败: %v", err)
		os.Exit(1)
	}
	log.Println("已关闭")
}
