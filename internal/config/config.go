// Package config 读取环境变量配置。
package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	HTTPAddr       string // HTTP 监听地址
	DatabaseURL    string // pgx 连接串
	StorageRoot    string // 模拟节点目录的根目录
	DataShards     int    // 默认数据分片数
	ParityShards   int    // 默认校验分片数（容错数量上限）
	MaxObjectBytes int64  // 单次上传对象大小上限
}

func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:       env("HTTP_ADDR", ":8080"),
		DatabaseURL:    env("DATABASE_URL", "host=/tmp port=55432 user=postgres dbname=erasure_store sslmode=disable"),
		StorageRoot:    env("STORAGE_ROOT", "./data"),
		DataShards:     envInt("DATA_SHARDS", 4),
		ParityShards:   envInt("PARITY_SHARDS", 2),
		MaxObjectBytes: envInt64("MAX_OBJECT_BYTES", 64<<20),
	}
	if cfg.DataShards < 1 {
		return cfg, fmt.Errorf("DATA_SHARDS 必须 >= 1，当前为 %d", cfg.DataShards)
	}
	if cfg.ParityShards < 1 {
		return cfg, fmt.Errorf("PARITY_SHARDS 必须 >= 1，当前为 %d", cfg.ParityShards)
	}
	if cfg.MaxObjectBytes < 1 {
		return cfg, fmt.Errorf("MAX_OBJECT_BYTES 必须 >= 1，当前为 %d", cfg.MaxObjectBytes)
	}
	return cfg, nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}
