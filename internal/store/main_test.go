package store_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"erasure-store/internal/db"
	"erasure-store/internal/store"
)

// dsn 由 TestMain 指向为本测试包独立创建的一次性数据库，
// 避免与并行执行的 api 测试包共用同一库互相清理。
var dsn string

// adminDSN 允许用 TEST_DATABASE_ADMIN 覆盖（需有建库权限）；
// 默认连本机教学环境里 /tmp:55432 上的 postgres 库。
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
		log.Printf("跳过 store 集成测试：无法连接 PostgreSQL: %v", err)
		os.Exit(0) // 无数据库环境下整体跳过
	}
	if err := admin.Ping(ctx); err != nil {
		log.Printf("跳过 store 集成测试：PostgreSQL 不可用: %v", err)
		os.Exit(0)
	}

	var rb [4]byte
	_, _ = rand.Read(rb[:])
	name := "estore_store_test_" + hex.EncodeToString(rb[:])
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		log.Printf("跳过 store 集成测试：创建测试库失败: %v", err)
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

	// 先断开可能仍连接着的会话，再删库。
	_, _ = admin.Exec(ctx,
		`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1`, name)
	_, _ = admin.Exec(ctx, "DROP DATABASE "+name)
	admin.Close()
	os.Exit(code)
}

// newTestManager 构造一个指向全新临时目录的 Manager，并清空测试库的业务表。
// 节点状态会被全部重置为 up（用例不并行，避免 down 状态互相干扰）。
func newTestManager(t *testing.T, d, p int) (*store.Manager, *pgxpool.Pool, context.Context) {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("创建数据库连接失败: %v", err)
	}

	// 清理上一轮残留：分片布局、对象清单；节点全部恢复 up。
	if _, err := pool.Exec(ctx, `DELETE FROM shards`); err != nil {
		t.Fatalf("清理 shards 失败: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM objects`); err != nil {
		t.Fatalf("清理 objects 失败: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE nodes SET state = 'up'`); err != nil {
		t.Fatalf("重置节点状态失败: %v", err)
	}

	root := t.TempDir()
	mgr, err := store.NewManager(pool, root, d, p)
	if err != nil {
		t.Fatalf("NewManager 失败: %v", err)
	}
	if err := mgr.EnsureNodes(ctx, d+p); err != nil {
		t.Fatalf("EnsureNodes 失败: %v", err)
	}
	t.Cleanup(pool.Close)
	return mgr, pool, ctx
}
