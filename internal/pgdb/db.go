// Package pgdb 负责 PostgreSQL 持久化：对象清单、分片布局、逐块校验值。
//
// 关键设计：
//   - 对象“发布”= 清单行 + 全部块校验值在【同一个事务】里可见。
//     上传过程中任何步骤失败都会回滚，因此绝不会发布一份指向缺失分片、
//     实际无法读取的“完整清单”。
//   - 每个块记录 sha256（落盘内容，含补齐零）与块大小，恢复时逐块比对。
package pgdb

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

//go:embed schema.sql
var schemaSQL string

var (
	ErrNotFound      = errors.New("对象清单不存在")
	ErrAlreadyExists = errors.New("对象已存在")
)

type DB struct {
	conn *pgx.Conn
}

// Connect 连接数据库，必要时自动创建目标库（教学环境方便）。
func Connect(ctx context.Context, databaseURL string) (*DB, error) {
	cfg, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		// 目标数据库可能不存在：连到 postgres 库自动创建。
		if adminErr := createDatabaseIfMissing(ctx, databaseURL, cfg.Database); adminErr != nil {
			return nil, fmt.Errorf("连接数据库失败: %w (自动建库也失败: %v)", err, adminErr)
		}
		conn, err = pgx.ConnectConfig(ctx, cfg)
		if err != nil {
			return nil, err
		}
	}
	db := &DB{conn: conn}
	if err := db.Migrate(ctx); err != nil {
		_ = conn.Close(ctx)
		return nil, err
	}
	return db, nil
}

func createDatabaseIfMissing(ctx context.Context, databaseURL, dbName string) error {
	return ExecDDL(ctx, databaseURL, "/postgres", fmt.Sprintf(`CREATE DATABASE "%s"`, dbName))
}

// ExecDDL 在指定数据库（path 形如 "/postgres"）上执行不能放进事务的 DDL
// （CREATE/DROP DATABASE）。使用简单查询协议。
func ExecDDL(ctx context.Context, databaseURL, dbPath, statement string) error {
	u, err := url.Parse(databaseURL)
	if err != nil {
		return err
	}
	u.Path = dbPath
	cfg, err := pgx.ParseConfig(u.String())
	if err != nil {
		return err
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, statement)
	return err
}

// ExecDDLOnURL 是 ExecDDL 的导出版本（库路径由调用方在 statement 前自行切换）。
func ExecDDLOnURL(ctx context.Context, databaseURL, dbPath, statement string) error {
	return ExecDDL(ctx, databaseURL, dbPath, statement)
}

func (d *DB) Close(ctx context.Context) error { return d.conn.Close(ctx) }

// Exec 执行任意 SQL（测试/建库辅助用）。
func (d *DB) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := d.conn.Exec(ctx, sql, args...)
	return err
}

func (d *DB) Migrate(ctx context.Context) error {
	_, err := d.conn.Exec(ctx, schemaSQL)
	return err
}

// Object 是 objects 表中的一行清单。
type Object struct {
	ID           string
	Size         int64
	DataShards   int
	ParityShards int
	BlockSize    int
	Stripes      int
	TailPadding  int    // 对象末尾补齐的零字节数；读取时必须截断
	SHA256       string // 原始对象内容的 sha256（不含补齐）
	CreatedAt    time.Time
}

// BlockChecksum 是一个分片块的权威校验记录。
type BlockChecksum struct {
	ObjectID string
	Stripe   int
	Node     int
	Size     int
	SHA256   string // 落盘块内容的 sha256（含条带内补零）
}

// BlockKey 用于按 (条带, 节点) 查校验值。
type BlockKey struct {
	Stripe int
	Node   int
}

// InsertManifestInput 是一次清单发布的全部输入。
type InsertManifestInput struct {
	Object Object
	Blocks []BlockChecksum
}

// Tx 暴露给 service 的事务句柄。
type Tx struct{ t pgx.Tx }

func (d *DB) Begin(ctx context.Context) (*Tx, error) {
	t, err := d.conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &Tx{t: t}, nil
}

func (tx *Tx) Commit(ctx context.Context) error   { return tx.t.Commit(ctx) }
func (tx *Tx) Rollback(ctx context.Context) error { return tx.t.Rollback(ctx) }

// InsertManifest 在事务内写入清单 + 全部块校验值。
// 块通过 Copy 批量写入；任何一步失败，事务回滚，清单对外永远不可见。
func (d *DB) InsertManifest(ctx context.Context, tx *Tx, in InsertManifestInput) error {
	o := in.Object
	_, err := tx.t.Exec(ctx, `
INSERT INTO objects (id, size, data_shards, parity_shards, block_size, stripes, tail_padding, sha256)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		o.ID, o.Size, o.DataShards, o.ParityShards, o.BlockSize, o.Stripes, o.TailPadding, o.SHA256)
	if err != nil {
		return mapPgErr(err, o.ID)
	}

	if len(in.Blocks) > 0 {
		rows := make([][]any, len(in.Blocks))
		for i, b := range in.Blocks {
			rows[i] = []any{b.ObjectID, b.Stripe, b.Node, b.Size, b.SHA256}
		}
		ct, err := tx.t.CopyFrom(ctx,
			pgx.Identifier{"shard_blocks"},
			[]string{"object_id", "stripe", "node", "size", "sha256"},
			pgx.CopyFromRows(rows))
		if err != nil {
			return err
		}
		if ct != int64(len(rows)) {
			return fmt.Errorf("块校验值写入数量 %d != %d", ct, len(rows))
		}
	}
	return nil
}

func mapPgErr(err error, id string) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
		return fmt.Errorf("%w: %s", ErrAlreadyExists, id)
	}
	return err
}

// GetObject 读取清单；不存在返回 ErrNotFound。
func (d *DB) GetObject(ctx context.Context, id string) (Object, error) {
	var o Object
	err := d.conn.QueryRow(ctx, `
SELECT id, size, data_shards, parity_shards, block_size, stripes, tail_padding, sha256, created_at
FROM objects WHERE id=$1`, id).Scan(
		&o.ID, &o.Size, &o.DataShards, &o.ParityShards, &o.BlockSize,
		&o.Stripes, &o.TailPadding, &o.SHA256, &o.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Object{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return o, err
}

// BlockChecksums 返回该对象全部块的校验值，按 (stripe, node) 排序。
func (d *DB) BlockChecksums(ctx context.Context, id string) (map[BlockKey]BlockChecksum, error) {
	rows, err := d.conn.Query(ctx, `
SELECT object_id, stripe, node, size, sha256
FROM shard_blocks WHERE object_id=$1 ORDER BY stripe, node`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	m := make(map[BlockKey]BlockChecksum)
	for rows.Next() {
		var b BlockChecksum
		if err := rows.Scan(&b.ObjectID, &b.Stripe, &b.Node, &b.Size, &b.SHA256); err != nil {
			return nil, err
		}
		m[BlockKey{Stripe: b.Stripe, Node: b.Node}] = b
	}
	return m, rows.Err()
}

// ListObjects 简单清单（教学用）。
type ListEntry struct {
	ID        string    `json:"id"`
	Size      int64     `json:"size"`
	Stripes   int       `json:"stripes"`
	SHA256    string    `json:"sha256"`
	CreatedAt time.Time `json:"created_at"`
}

func (d *DB) ListObjects(ctx context.Context, limit int) ([]ListEntry, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	rows, err := d.conn.Query(ctx, `
SELECT id, size, stripes, sha256, created_at FROM objects ORDER BY created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ListEntry
	for rows.Next() {
		var e ListEntry
		if err := rows.Scan(&e.ID, &e.Size, &e.Stripes, &e.SHA256, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
