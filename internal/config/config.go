// Package config holds runtime configuration for ecstore.
//
// 纠删码拓扑可配置：DataShards(数据分片数)、ParityShards(校验分片数)、
// BlockSize(每个条带内单个分片块的字节数)。
package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	ListenAddr   string
	DatabaseURL  string
	NodesRoot    string
	DataShards   int
	ParityShards int
	BlockSize    int
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s=%q 不是整数: %w", key, v, err)
	}
	return n, nil
}

// Load 从环境变量读取配置（均带默认值，方便教学演示）。
func Load() (Config, error) {
	c := Config{}
	var err error
	c.ListenAddr = getenv("EC_LISTEN", ":8080")
	c.DatabaseURL = getenv("EC_DATABASE_URL",
		"postgres://ecuser@127.0.0.1:5432/ecstore?sslmode=disable")
	c.NodesRoot = getenv("EC_NODES_ROOT", "./nodes")
	if c.DataShards, err = getenvInt("EC_DATA_SHARDS", 4); err != nil {
		return c, err
	}
	if c.ParityShards, err = getenvInt("EC_PARITY_SHARDS", 2); err != nil {
		return c, err
	}
	if c.BlockSize, err = getenvInt("EC_BLOCK_SIZE", 64*1024); err != nil {
		return c, err
	}
	if c.DataShards <= 0 {
		return c, fmt.Errorf("EC_DATA_SHARDS 必须 > 0，当前 %d", c.DataShards)
	}
	if c.ParityShards <= 0 {
		return c, fmt.Errorf("EC_PARITY_SHARDS 必须 > 0，当前 %d", c.ParityShards)
	}
	if c.BlockSize <= 0 {
		return c, fmt.Errorf("EC_BLOCK_SIZE 必须 > 0，当前 %d", c.BlockSize)
	}
	return c, nil
}

// TotalShards 返回节点总数 D+P。
func (c Config) TotalShards() int { return c.DataShards + c.ParityShards }

// FaultTolerance 返回最多可容忍的失效分片数 P。
func (c Config) FaultTolerance() int { return c.ParityShards }
