// ecstore 是一个纠删码对象存储教学服务：
// 多目录模拟独立节点，klauspost/reedsolomon 编码，PostgreSQL 存清单与校验值。
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ecstore/internal/api"
	"ecstore/internal/config"
	"ecstore/internal/erasure"
	"ecstore/internal/nodestore"
	"ecstore/internal/pgdb"
	"ecstore/internal/service"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("配置错误: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	db, err := pgdb.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("连接 PostgreSQL 失败: %v", err)
	}
	defer db.Close(context.Background())
	log.Printf("PostgreSQL 已连接，schema 已就绪")

	codec, err := erasure.New(cfg.DataShards, cfg.ParityShards, cfg.BlockSize)
	if err != nil {
		log.Fatalf("初始化纠删码器失败: %v", err)
	}

	nodes, err := nodestore.New(cfg.NodesRoot, cfg.TotalShards())
	if err != nil {
		log.Fatalf("初始化节点目录失败: %v", err)
	}
	log.Printf("节点目录就绪: %s (D=%d P=%d block=%d, 共 %d 个节点, 容错 %d)",
		cfg.NodesRoot, cfg.DataShards, cfg.ParityShards, cfg.BlockSize,
		cfg.TotalShards(), cfg.FaultTolerance())

	svc := service.New(db, nodes, codec)
	h := api.New(svc, cfg.TotalShards())

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           h.Mux(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		log.Printf("HTTP 监听 %s", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP 服务失败: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("收到退出信号，正在关闭...")
	shCtx, shCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shCancel()
	if err := srv.Shutdown(shCtx); err != nil {
		log.Printf("HTTP 关闭超时: %v", err)
		os.Exit(1)
	}
	log.Printf("已退出")
}
