package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/klauspost/reedsolomon"
)

// Manager 组合 PostgreSQL 清单库与本地目录节点，提供纠删码存储能力。
type Manager struct {
	pool     *pgxpool.Pool
	root     string // 节点根目录：root/node0、root/node1 ...
	defaultD int
	defaultP int

	encMu sync.Mutex
	encs  map[[2]int]reedsolomon.Encoder // 按 (数据分片数, 校验分片数) 缓存编码器

	objLocks *keyedLocks // 单对象粒度互斥，避免并发重建相互踩踏

	// 测试专用钩子（非测试场景保持 nil）
	TestHooks *TestHooks
}

func NewManager(pool *pgxpool.Pool, storageRoot string, dataShards, parityShards int) (*Manager, error) {
	if dataShards < 1 || parityShards < 1 {
		return nil, fmt.Errorf("数据分片数和校验分片数都必须 >= 1")
	}
	if err := os.MkdirAll(storageRoot, 0o755); err != nil {
		return nil, fmt.Errorf("创建存储根目录失败: %w", err)
	}
	return &Manager{
		pool:     pool,
		root:     storageRoot,
		defaultD: dataShards,
		defaultP: parityShards,
		encs:     make(map[[2]int]reedsolomon.Encoder),
		objLocks: newKeyedLocks(),
	}, nil
}

func (m *Manager) encoder(d, p int) (reedsolomon.Encoder, error) {
	key := [2]int{d, p}
	m.encMu.Lock()
	defer m.encMu.Unlock()
	if enc, ok := m.encs[key]; ok {
		return enc, nil
	}
	enc, err := reedsolomon.New(d, p, reedsolomon.WithAutoGoroutines(defaultShardSizeHint))
	if err != nil {
		return nil, fmt.Errorf("创建 Reed-Solomon 编码器失败: %w", err)
	}
	m.encs[key] = enc
	return enc, nil
}

// defaultShardSizeHint 仅用于 reedsolomon 自动选择 goroutine 数量的提示。
const defaultShardSizeHint = 1 << 20

// ---- 路径约定 ----
//
//	root/node<N>/<前2位id>/<id>.shard<idx>           最终分片文件
//	root/node<N>/staging/<前2位id>/<id>.shard<idx>   上传暂存文件
func (m *Manager) objectDir(node int, id string) string {
	return filepath.Join(m.root, fmt.Sprintf("node%d", node), id[:2])
}

func (m *Manager) finalPath(node int, id string, idx int) string {
	return filepath.Join(m.objectDir(node, id), fmt.Sprintf("%s.shard%d", id, idx))
}

func (m *Manager) stagingPath(node int, id string, idx int) string {
	return filepath.Join(m.root, fmt.Sprintf("node%d", node), "staging", id[:2], fmt.Sprintf("%s.shard%d", id, idx))
}

func shardHex(sum [sha256.Size]byte) string { return hex.EncodeToString(sum[:]) }

func sha256Hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// EnsureNodes 在启动时为 0..total-1 号节点补登记，并创建对应目录。
func (m *Manager) EnsureNodes(ctx context.Context, total int) error {
	for n := 0; n < total; n++ {
		if _, err := m.pool.Exec(ctx,
			`INSERT INTO nodes(node_id, state) VALUES ($1, 'up')
			 ON CONFLICT (node_id) DO NOTHING`, n); err != nil {
			return fmt.Errorf("登记节点 %d 失败: %w", n, err)
		}
		if err := os.MkdirAll(filepath.Join(m.root, fmt.Sprintf("node%d", n)), 0o755); err != nil {
			return fmt.Errorf("创建节点 %d 目录失败: %w", n, err)
		}
	}
	return nil
}

// downNodes 返回处于 down 状态的节点集合。
func (m *Manager) downNodes(ctx context.Context) (map[int]bool, error) {
	rows, err := m.pool.Query(ctx, `SELECT node_id FROM nodes WHERE state = 'down'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	down := map[int]bool{}
	for rows.Next() {
		var n int
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		down[n] = true
	}
	return down, rows.Err()
}

// allNodeStates 返回 node_id -> state。
func (m *Manager) allNodeStates(ctx context.Context) (map[int]string, error) {
	rows, err := m.pool.Query(ctx, `SELECT node_id, state FROM nodes`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	states := map[int]string{}
	for rows.Next() {
		var (
			n int
			s string
		)
		if err := rows.Scan(&n, &s); err != nil {
			return nil, err
		}
		states[n] = s
	}
	return states, rows.Err()
}

// writeFileAtomic 将内容写入临时文件 fsync 后再 rename 到 dst，
// 保证读取端要么看到完整的新文件，要么看不到文件。
func writeFileAtomic(dst string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	tmp := dst + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, dst)
}

// ---- 简易 keyed 互斥锁：同一对象 ID 串行化上传/重建 ----
//
// 所有请求经一个串行 goroutine 处理，持锁状态仅在该 goroutine 内变更，
// 因此天然无竞态。

type klReq struct {
	key  string
	ch   chan struct{}
	lock bool // true=加锁，false=解锁
}

type keyedLocks struct {
	req chan klReq
}

func newKeyedLocks() *keyedLocks {
	k := &keyedLocks{req: make(chan klReq)}
	go func() {
		held := map[string]bool{}
		waiters := map[string][]chan struct{}{}
		for r := range k.req {
			if r.lock {
				if !held[r.key] {
					held[r.key] = true
					close(r.ch)
				} else {
					waiters[r.key] = append(waiters[r.key], r.ch)
				}
			} else {
				if ws := waiters[r.key]; len(ws) > 0 {
					close(ws[0])
					waiters[r.key] = ws[1:]
				} else {
					delete(held, r.key)
				}
			}
		}
	}()
	return k
}

func (k *keyedLocks) Lock(key string) {
	ch := make(chan struct{})
	k.req <- klReq{key: key, ch: ch, lock: true}
	<-ch
}

func (k *keyedLocks) Unlock(key string) {
	k.req <- klReq{key: key, lock: false}
}

// joinObject 按分片编号顺序拼接数据分片，并截掉尾部补齐零。
func joinObject(shards [][]byte, dataShards int, size int64) []byte {
	use := make([]byte, 0, int64(dataShards)*int64(len(shards[0])))
	for i := 0; i < dataShards; i++ {
		use = append(use, shards[i]...)
	}
	return use[:size]
}
