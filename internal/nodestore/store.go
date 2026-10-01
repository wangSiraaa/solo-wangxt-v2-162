// Package nodestore 用磁盘上的多个独立目录模拟存储节点。
//
// 目录布局：
//
//	<root>/node-00/shards/<oid>        已提交的对象分片（块顺序拼接）
//	<root>/node-00/tmp/<oid>.tmp       上传中的临时分片（提交后原子 rename）
//	<root>/node-00/DOWN                存在该文件时模拟“节点不可用”
//
// 两种故障严格区分：
//   - 节点不可用：目录有 DOWN 标记 / IO 错误 / 分片文件缺失（节点活着但读不到）。
//   - 内容损坏：节点可访问、字节能读出，但校验值与清单记录不符。
package nodestore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	ErrNodeDown      = errors.New("节点不可用")
	ErrShardMissing  = errors.New("分片在该节点上缺失")
	ErrInvalidObject = errors.New("非法对象 ID")
)

// Store 管理 total 个模拟节点。
type Store struct {
	root  string
	total int
	mu    sync.Mutex // 串行化同路径的临时文件操作，教学项目够用
}

func New(root string, total int) (*Store, error) {
	if total <= 0 {
		return nil, fmt.Errorf("节点数必须 > 0")
	}
	s := &Store{root: root, total: total}
	for j := 0; j < total; j++ {
		for _, sub := range []string{"shards", "tmp"} {
			if err := os.MkdirAll(s.nodePath(j, sub), 0o755); err != nil {
				return nil, fmt.Errorf("创建节点 %d 目录失败: %w", j, err)
			}
		}
	}
	if err := s.CleanStaging(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) Total() int { return s.total }

// validateID 防止路径逃逸。
func validateID(id string) error {
	if id == "" || len(id) > 200 {
		return ErrInvalidObject
	}
	if strings.ContainsAny(id, "/\\\x00") || id == "." || id == ".." || strings.HasPrefix(id, "..") {
		return ErrInvalidObject
	}
	return nil
}

func (s *Store) nodePath(node int, parts ...string) string {
	p := []string{s.root, fmt.Sprintf("node-%02d", node)}
	return filepath.Join(append(p, parts...)...)
}

// IsUp 返回节点是否处于可用状态（DOWN 标记文件存在即下线）。
func (s *Store) IsUp(node int) bool {
	if node < 0 || node >= s.total {
		return false
	}
	_, err := os.Stat(s.nodePath(node, "DOWN"))
	return errors.Is(err, os.ErrNotExist)
}

// SetNodeDown / SetNodeUp 模拟节点掉电、网络隔离与恢复。
func (s *Store) SetNodeDown(node int, reason string) error {
	if node < 0 || node >= s.total {
		return fmt.Errorf("节点编号 %d 越界(0..%d)", node, s.total-1)
	}
	f, err := os.Create(s.nodePath(node, "DOWN"))
	if err != nil {
		return err
	}
	_, _ = f.WriteString(reason)
	return f.Close()
}

func (s *Store) SetNodeUp(node int) error {
	if node < 0 || node >= s.total {
		return fmt.Errorf("节点编号 %d 越界(0..%d)", node, s.total-1)
	}
	err := os.Remove(s.nodePath(node, "DOWN"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// ---- 上传：先写临时分片，全部落盘后再原子提交 ----

// AppendStaging 把某个条带中 node 节点的块追加到临时分片。
func (s *Store) AppendStaging(oid string, node, stripe int, block []byte) error {
	if err := validateID(oid); err != nil {
		return err
	}
	if !s.IsUp(node) {
		return fmt.Errorf("%w: node-%02d", ErrNodeDown, node)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// O_APPEND 保证每个条带块顺序拼接到分片文件末尾。
	f, err := os.OpenFile(s.nodePath(node, "tmp", oid+".tmp"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	// 条带必须按序写入；用当前文件大小校验位置，防止乱序造成块错位。
	info, _ := f.Stat()
	want := int64(stripe * len(block))
	if info != nil && info.Size() != want {
		return fmt.Errorf("node-%02d 临时分片大小 %d != 预期偏移 %d（条带乱序）", node, info.Size(), want)
	}
	if _, err := f.Write(block); err != nil {
		return err
	}
	return nil
}

// FlushStaging fsync 一个节点上的临时分片。
func (s *Store) FlushStaging(oid string, node int) error {
	if !s.IsUp(node) {
		return fmt.Errorf("%w: node-%02d", ErrNodeDown, node)
	}
	f, err := os.Open(s.nodePath(node, "tmp", oid+".tmp"))
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// CommitStaging 把临时分片原子重命名为正式分片。
// 只允许在数据库清单事务提交之前调用 —— 清单提交后失败也无法产生“指向不存在分片”的记录。
func (s *Store) CommitStaging(oid string, node int) error {
	if err := validateID(oid); err != nil {
		return err
	}
	if !s.IsUp(node) {
		return fmt.Errorf("%w: node-%02d", ErrNodeDown, node)
	}
	return os.Rename(s.nodePath(node, "tmp", oid+".tmp"), s.nodePath(node, "shards", oid))
}

// AbortStaging 上传中断/失败时清理所有节点上的临时分片。
func (s *Store) AbortStaging(oid string) {
	_ = validateID(oid)
	for node := 0; node < s.total; node++ {
		_ = os.Remove(s.nodePath(node, "tmp", oid+".tmp"))
	}
}

// ReserveStaging 检查 oid 是否从未占用（临时或正式），防止并发/重复上传。
func (s *Store) ReserveStaging(oid string) error {
	if err := validateID(oid); err != nil {
		return err
	}
	for node := 0; node < s.total; node++ {
		for _, p := range []string{
			s.nodePath(node, "tmp", oid+".tmp"),
			s.nodePath(node, "shards", oid),
		} {
			if _, err := os.Stat(p); err == nil {
				return fmt.Errorf("对象 %q 在 node-%02d 已存在", oid, node)
			}
		}
	}
	return nil
}

// CleanStaging 启动时清理上一进程遗留的临时分片（这些上传从未提交清单）。
func (s *Store) CleanStaging() error {
	for node := 0; node < s.total; node++ {
		entries, err := os.ReadDir(s.nodePath(node, "tmp"))
		if err != nil {
			return err
		}
		for _, e := range entries {
			_ = os.Remove(s.nodePath(node, "tmp", e.Name()))
		}
	}
	return nil
}

// ReadBlock 读取对象在 node 节点上第 stripe 个块。
// 返回 ErrNodeDown（节点下线/IO 故障）或 ErrShardMissing（节点正常但文件缺失）；
// 能读到字节时不做校验 —— 校验由 service 层完成，以区分“内容损坏”。
func (s *Store) ReadBlock(oid string, node, stripe, blockSize int) ([]byte, error) {
	if err := validateID(oid); err != nil {
		return nil, err
	}
	if node < 0 || node >= s.total {
		return nil, fmt.Errorf("%w: 非法节点 %d", ErrNodeDown, node)
	}
	if !s.IsUp(node) {
		return nil, fmt.Errorf("%w: node-%02d 被标记下线", ErrNodeDown, node)
	}
	path := s.nodePath(node, "shards", oid)
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: node-%02d 上无 %q", ErrShardMissing, node, oid)
		}
		return nil, fmt.Errorf("%w: node-%02d 打开失败: %v", ErrNodeDown, node, err)
	}
	defer f.Close()

	buf := make([]byte, blockSize)
	off := int64(stripe * blockSize)
	n, err := f.ReadAt(buf, off)
	if err != nil && err.Error() != "EOF" && !errors.Is(err, os.ErrNotExist) {
		// 短读在最后条带也可能发生吗？不会——每个块都是定长 BlockSize（含补零）。
		// 因此读不满即视为节点 IO 故障。
		return nil, fmt.Errorf("%w: node-%02d 读取失败: %v", ErrNodeDown, node, err)
	}
	if n != blockSize {
		return nil, fmt.Errorf("%w: node-%02d 分片长度不足(%d/%d)", ErrShardMissing, node, n, blockSize)
	}
	return buf, nil
}

// WriteBlock 把重建出的块写回（覆盖写）到指定节点的分片对应位置。
func (s *Store) WriteBlock(oid string, node, stripe, blockSize int, block []byte) error {
	if err := validateID(oid); err != nil {
		return err
	}
	if !s.IsUp(node) {
		return fmt.Errorf("%w: node-%02d", ErrNodeDown, node)
	}
	if len(block) != blockSize {
		return fmt.Errorf("回写块长度 %d != %d", len(block), blockSize)
	}
	f, err := os.OpenFile(s.nodePath(node, "shards", oid), os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteAt(block, int64(stripe*blockSize))
	if err != nil {
		return err
	}
	return f.Sync()
}

// EnsureShardForRepair 在目标分片完全丢失时创建定长文件，便于块级回写。
func (s *Store) EnsureShardForRepair(oid string, node, stripes, blockSize int) error {
	if !s.IsUp(node) {
		return fmt.Errorf("%w: node-%02d", ErrNodeDown, node)
	}
	path := s.nodePath(node, "shards", oid)
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Truncate(int64(stripes * blockSize)); err != nil {
		return err
	}
	return f.Sync()
}

// ShardExists 判断某节点上是否有正式分片（忽略节点上下线状态）。
func (s *Store) ShardExists(oid string, node int) bool {
	_, err := os.Stat(s.nodePath(node, "shards", oid))
	return err == nil
}

// RemoveShard 模拟“该节点上的这份分片彻底丢失”（区别于下线：节点仍在线）。
func (s *Store) RemoveShard(oid string, node int) error {
	if node < 0 || node >= s.total {
		return fmt.Errorf("节点编号 %d 越界", node)
	}
	err := os.Remove(s.nodePath(node, "shards", oid))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// CorruptBlock 翻转块内一字节，模拟静默数据损坏(bit rot / 脏盘)。
// 该操作不触碰 DOWN 标记 —— 节点完全在线，只是内容错了。
func (s *Store) CorruptBlock(oid string, node, stripe, blockSize int) error {
	if !s.IsUp(node) {
		return fmt.Errorf("%w: node-%02d（先恢复节点再注入内容损坏）", ErrNodeDown, node)
	}
	path := s.nodePath(node, "shards", oid)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	off := int64(stripe*blockSize) + (int64(stripe) % int64(blockSize)) // 选块内一个确定位置
	b := make([]byte, 1)
	if _, err := f.ReadAt(b, off); err != nil {
		return err
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b, off); err != nil {
		return err
	}
	return f.Sync()
}

// ShardSize 返回分片文件大小，节点下线或文件不存在时返回 0 与 false。
func (s *Store) ShardSize(oid string, node int) (int64, bool) {
	if !s.IsUp(node) {
		return 0, false
	}
	info, err := os.Stat(s.nodePath(node, "shards", oid))
	if err != nil {
		return 0, false
	}
	return info.Size(), true
}
