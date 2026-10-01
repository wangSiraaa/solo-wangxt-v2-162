package api_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"erasure-store/internal/db"
)

// dsn 由 TestMain 指向 api 测试包独立的一次性数据库。
var dsn string

func adminDSN() string {
	if v := os.Getenv("TEST_DATABASE_ADMIN"); v != "" {
		return v
	}
	return "host=/tmp port=55432 user=postgres dbname=postgres sslmode=disable"
}

func TestMain(m *testing.M) {
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, adminDSN())
	if err != nil {
		log.Printf("跳过 api 集成测试：无法连接 PostgreSQL: %v", err)
		os.Exit(0)
	}
	if err := admin.Ping(ctx); err != nil {
		log.Printf("跳过 api 集成测试：PostgreSQL 不可用: %v", err)
		os.Exit(0)
	}

	var rb [4]byte
	_, _ = rand.Read(rb[:])
	name := "estore_api_test_" + hex.EncodeToString(rb[:])
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		log.Printf("跳过 api 集成测试：创建测试库失败: %v", err)
		os.Exit(0)
	}

	dsn = "host=/tmp port=55432 user=postgres dbname=" + name + " sslmode=disable"
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("连接测试库失败: %v", err)
	}
	if err := db.Migrate(ctx, pool); err != nil {
		log.Fatalf("测试库迁移失败: %v", err)
	}
	pool.Close()

	code := m.Run()

	_, _ = admin.Exec(ctx,
		`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1`, name)
	_, _ = admin.Exec(ctx, "DROP DATABASE "+name)
	admin.Close()
	os.Exit(code)
}
