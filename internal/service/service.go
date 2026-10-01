// Package service 是纠删码存储的业务核心：上传发布、范围读取、
// 条带级重建与逐块校验、健康检查、主动修复与故障注入。
package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"ecstore/internal/erasure"
	"ecstore/internal/nodestore"
	"ecstore/internal/pgdb"
)

// 块状态：故障检测必须把“节点不可用”和“内容损坏”分开报告。
const (
	BlockOK          = "ok"          // 可读取且校验通过
	BlockUnavailable = "unavailable" // 节点下线 / IO 故障（读不到字节）
	BlockMissing     = "missing"     // 节点在线但分片/块不存在
	BlockCorrupt     = "corrupt"     // 能读到字节，但校验值不符
	BlockRebuilt     = "rebuilt"     // 本次修复中重建并写回成功
)

// UnrecoverableError 表示当前有效分片数 < 数据分片数 D，数学上无法恢复。
type UnrecoverableError struct {
	ObjectID    string `json:"object_id"`
	Stripe      int    `json:"stripe"`
	Good        int    `json:"good_blocks"`
	DataNeeded  int    `json:"data_shards_needed"`
	Unavailable []int  `json:"unavailable_nodes"`
	Corrupt     []int  `json:"corrupt_nodes"`
	Missing     []int  `json:"missing_nodes"`
}

func (e *UnrecoverableError) Error() string {
	return fmt.Sprintf(
		"对象 %q 的条带 %d 不可恢复: 有效块 %d < 数据分片数 %d（不可用节点 %v, 损坏 %v, 丢失 %v）",
		e.ObjectID, e.Stripe, e.Good, e.DataNeeded, e.Unavailable, e.Corrupt, e.Missing)
}

// BlockInfo 描述一个块的检测结果。
type BlockInfo struct {
	Node   int    `json:"node"`
	Stripe int    `json:"stripe"`
	Status string `json:"status"`
}

// ErrObjectBusy 表示已有同 ID 的上传在进行，或对象/分片已存在。
var ErrObjectBusy = errors.New("对象 ID 已被占用")

type Service struct {
	db    *pgdb.DB
	nodes *nodestore.Store
	codec *erasure.Codec

	// uploadLocks 串行化同一 oid 的上传，避免并发同名上传互相覆盖分片
	// （跨多个 ecstore 进程的同名竞争由 DB 主键兜底，教学场景单进程即可）。
	uploadMu   sync.Mutex
	uploadLock map[string]*sync.Mutex
}

func New(db *pgdb.DB, nodes *nodestore.Store, codec *erasure.Codec) *Service {
	return &Service{db: db, nodes: nodes, codec: codec, uploadLock: make(map[string]*sync.Mutex)}
}

func (s *Service) lockOID(oid string) func() {
	s.uploadMu.Lock()
	mu, ok := s.uploadLock[oid]
	if !ok {
		mu = &sync.Mutex{}
		s.uploadLock[oid] = mu
	}
	s.uploadMu.Unlock()
	mu.Lock()
	return func() { mu.Unlock() }
}

// ---------------- 上传 ----------------

// Upload 读取 r 的全部字节，编码成 D+P 个分片并发布清单。
//
// 发布顺序（保证绝不发布“实际无法读取的完整清单”）：
//  1. 预留 oid（临时/正式分片均不得存在）
//  2. 逐条带编码 → 写入各节点 tmp 临时分片 → fsync
//  3. 临时分片原子 rename 为正式分片（此时清单尚不存在）
//  4. 单事务写入清单 + 全部块校验值并 commit
//
// 任一步失败：回滚事务、清理临时/已重命名分片，清单永远不出现。
func (s *Service) Upload(ctx context.Context, oid string, r io.Reader) (pgdb.Object, error) {
	unlock := s.lockOID(oid)
	defer unlock()

	if err := s.nodes.ReserveStaging(oid); err != nil {
		return pgdb.Object{}, fmt.Errorf("%w: %v", ErrObjectBusy, err)
	}

	// bodyReader 区分“HTTP 请求体正常读完(EOF)”与“客户端中途断连(其它错误)”。
	// 即便断连恰好发生在条带边界，也绝不能把残缺上传当作完整对象发布。
	br := &bodyReader{r: r}

	objHash := sha256.New()
	tr := io.TeeReader(br, objHash)

	blockRecords := make([]pgdb.BlockChecksum, 0)
	cleanup := func() { s.nodes.AbortStaging(oid) }

	size, stripeCount, tailPad, err := s.codec.EncodeStream(tr, func(stripe int, blocks [][]byte) error {
		for node := 0; node < s.codec.TotalShards(); node++ {
			if err := s.nodes.AppendStaging(oid, node, stripe, blocks[node]); err != nil {
				return err
			}
			h := sha256.Sum256(blocks[node])
			blockRecords = append(blockRecords, pgdb.BlockChecksum{
				ObjectID: oid, Stripe: stripe, Node: node,
				Size: len(blocks[node]), SHA256: hex.EncodeToString(h[:]),
			})
		}
		return nil
	})
	if err != nil {
		cleanup()
		return pgdb.Object{}, fmt.Errorf("编码/写分片失败: %w", err)
	}
	if br.err != nil {
		cleanup()
		return pgdb.Object{}, fmt.Errorf("上传中断，清单未发布(已写 %d 字节后连接失败: %w)", size, br.err)
	}

	// 所有块写完后 fsync 每个节点的临时文件。
	// 空对象没有任何条带（没有分片文件），跳过文件步骤直接发布空清单。
	if stripeCount == 0 {
		return s.publishManifest(ctx, oid, pgdb.Object{
			ID:           oid,
			Size:         0,
			DataShards:   s.codec.DataShards(),
			ParityShards: s.codec.ParityShards(),
			BlockSize:    s.codec.BlockSize(),
			Stripes:      0,
			TailPadding:  0,
			SHA256:       hex.EncodeToString(objHash.Sum(nil)), // sha256("")
		}, nil)
	}

	for node := 0; node < s.codec.TotalShards(); node++ {
		if err := s.nodes.FlushStaging(oid, node); err != nil {
			cleanup()
			return pgdb.Object{}, fmt.Errorf("node-%02d fsync 失败: %w", node, err)
		}
	}

	// 原子重命名（清单未提交，外部尚不可见此对象）。
	for node := 0; node < s.codec.TotalShards(); node++ {
		if err := s.nodes.CommitStaging(oid, node); err != nil {
			// 部分节点可能已 rename 成功；统一把正式分片也删掉。
			s.abortCommitted(oid)
			cleanup()
			return pgdb.Object{}, fmt.Errorf("node-%02d 分片提交失败: %w", node, err)
		}
	}

	obj := pgdb.Object{
		ID:           oid,
		Size:         size,
		DataShards:   s.codec.DataShards(),
		ParityShards: s.codec.ParityShards(),
		BlockSize:    s.codec.BlockSize(),
		Stripes:      stripeCount,
		TailPadding:  tailPad, // 读取时按对象大小截断，补零绝不返回给客户端
		SHA256:       hex.EncodeToString(objHash.Sum(nil)),
	}
	return s.publishManifest(ctx, oid, obj, blockRecords)
}

// publishManifest 是发布的最后一步：单事务写入清单 + 全部块校验值。
// 事务失败时删除所有已落盘的正式/临时分片，绝不遗留“清单指向缺失分片”的状态。
func (s *Service) publishManifest(ctx context.Context, oid string, obj pgdb.Object, blocks []pgdb.BlockChecksum) (pgdb.Object, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		s.abortCommitted(oid)
		s.nodes.AbortStaging(oid)
		return pgdb.Object{}, err
	}
	if err := s.db.InsertManifest(ctx, tx, pgdb.InsertManifestInput{Object: obj, Blocks: blocks}); err != nil {
		_ = tx.Rollback(ctx)
		s.abortCommitted(oid)
		s.nodes.AbortStaging(oid)
		return pgdb.Object{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		s.abortCommitted(oid)
		s.nodes.AbortStaging(oid)
		return pgdb.Object{}, fmt.Errorf("清单事务提交失败: %w", err)
	}
	return obj, nil
}

// abortCommitted 删除可能已 rename 成正式分片的文件。
func (s *Service) abortCommitted(oid string) {
	for node := 0; node < s.codec.TotalShards(); node++ {
		_ = s.nodes.RemoveShard(oid, node)
	}
}

// ---------------- 读取 ----------------

// ReadRange 读取对象 [start, start+length) 区间（length<=0 读到末尾）。
// 数据按需经条带重建获得；每块都对照清单校验值验证。
// 区间跨越的每个条带若有效块 < D，返回 *UnrecoverableError。
// 调用方提供 w 用于流式写出，返回实际写出的字节数。
func (s *Service) ReadRange(ctx context.Context, oid string, start, length int64, w io.Writer) (int64, pgdb.Object, error) {
	obj, checksums, err := s.loadObject(ctx, oid)
	if err != nil {
		return 0, pgdb.Object{}, err
	}
	if start < 0 || start > obj.Size {
		return 0, obj, fmt.Errorf("range start=%d 越界(size=%d)", start, obj.Size)
	}
	end := obj.Size
	if length > 0 {
		end = start + length
		if end > obj.Size {
			end = obj.Size
		}
	}

	var written int64
	stride := int64(s.codec.DataShards() * s.codec.BlockSize())
	firstStripe := int(start / stride)
	lastStripe := int((end - 1) / stride)
	if end == start { // 空区间（含空对象）
		return 0, obj, nil
	}

	for st := firstStripe; st <= lastStripe; st++ {
		logical, _, _, err := s.readStripe(ctx, obj, checksums, st)
		if err != nil {
			return written, obj, err
		}
		lo := int64(0)
		hi := int64(len(logical))
		stBase := int64(st) * stride
		if stBase < start {
			lo = start - stBase
		}
		if stBase+hi > end {
			hi = end - stBase
		}
		n, err := w.Write(logical[lo:hi])
		written += int64(n)
		if err != nil {
			return written, obj, err
		}
	}
	return written, obj, nil
}

// readStripe 采集一个条带的 D+P 个块，必要时重建；返回该条带的【逻辑数据】
// （已截断尾部补齐零）、全部块状态、以及重建后的完整块（供修复复用）。
func (s *Service) readStripe(ctx context.Context, obj pgdb.Object, checksums map[pgdb.BlockKey]pgdb.BlockChecksum, stripe int) (logical []byte, infos []BlockInfo, full [][]byte, err error) {
	D := s.codec.DataShards()
	T := s.codec.TotalShards()
	full = make([][]byte, T)
	infos = make([]BlockInfo, T)
	good := 0
	bad := 0

	for node := 0; node < T; node++ {
		infos[node] = BlockInfo{Node: node, Stripe: stripe}
		want, known := checksums[pgdb.BlockKey{Stripe: stripe, Node: node}]
		if !known {
			infos[node].Status = BlockMissing
			bad++
			continue
		}
		raw, rerr := s.nodes.ReadBlock(obj.ID, node, stripe, s.codec.BlockSize())
		if rerr != nil {
			switch {
			case errors.Is(rerr, nodestore.ErrNodeDown):
				infos[node].Status = BlockUnavailable
			case errors.Is(rerr, nodestore.ErrShardMissing):
				infos[node].Status = BlockMissing
			default:
				infos[node].Status = BlockUnavailable
			}
			full[node] = nil
			bad++
			continue
		}
		h := sha256.Sum256(raw)
		if hex.EncodeToString(h[:]) != want.SHA256 {
			infos[node].Status = BlockCorrupt
			full[node] = nil
			bad++
			continue
		}
		infos[node].Status = BlockOK
		full[node] = raw
		good++
	}

	rebuilt := bad > 0
	if rebuilt {
		if rerr := s.codec.ReconstructStripe(full); rerr != nil {
			return nil, infos, full, s.unrecoverable(obj.ID, stripe, infos)
		}
	}

	// 逐块校验：重建出来的块也必须与数据库里的权威校验值逐字节吻合。
	// RS 只能用“通过哈希的好块”做线性组合，因此重建结果理论上必然正确；
	// 再全量核对一次，任何意外都直接判不可恢复，绝不静默返回错误数据。
	if rebuilt {
		for node := 0; node < T; node++ {
			if infos[node].Status == BlockOK {
				continue
			}
			want, known := checksums[pgdb.BlockKey{Stripe: stripe, Node: node}]
			if !known || full[node] == nil {
				return nil, infos, full, s.unrecoverable(obj.ID, stripe, infos)
			}
			h := sha256.Sum256(full[node])
			if hex.EncodeToString(h[:]) != want.SHA256 {
				infos[node].Status = BlockCorrupt
				return nil, infos, full, s.unrecoverable(obj.ID, stripe, infos)
			}
		}
	}

	logical = s.logicalStripe(obj, stripe, full[:D])
	return logical, infos, full, nil
}

// logicalStripe 拼合 D 个数据块，并在“最后一个条带”去掉末尾补零。
func (s *Service) logicalStripe(obj pgdb.Object, stripe int, dataBlocks [][]byte) []byte {
	out := make([]byte, 0, s.codec.DataShards()*s.codec.BlockSize())
	for _, b := range dataBlocks {
		out = append(out, b...)
	}
	if stripe == obj.Stripes-1 && obj.TailPadding > 0 {
		out = out[:len(out)-obj.TailPadding]
	}
	return out
}

func (s *Service) unrecoverable(oid string, stripe int, infos []BlockInfo) error {
	e := &UnrecoverableError{ObjectID: oid, Stripe: stripe, DataNeeded: s.codec.DataShards()}
	for _, i := range infos {
		switch i.Status {
		case BlockOK:
			e.Good++
		case BlockUnavailable:
			e.Unavailable = append(e.Unavailable, i.Node)
		case BlockMissing:
			e.Missing = append(e.Missing, i.Node)
		case BlockCorrupt:
			e.Corrupt = append(e.Corrupt, i.Node)
		}
	}
	return e
}

// ReadAllVerified 读取整个对象，逐块校验之外再做端到端全对象 sha256 核对，
// 并通过 w 流式写出（同时喂给哈希器，不额外占内存）。
func (s *Service) ReadAllVerified(ctx context.Context, oid string, w io.Writer) (int64, error) {
	hw := hashWriter{h: sha256.New(), w: w}
	n, obj, err := s.ReadRange(ctx, oid, 0, 0, &hw)
	if err != nil {
		return n, err
	}
	got := hex.EncodeToString(hw.h.Sum(nil))
	if got != obj.SHA256 {
		return n, fmt.Errorf("对象 %q 端到端校验失败: 读到内容 sha256=%s, 清单记录=%s", oid, got, obj.SHA256)
	}
	return n, nil
}

// hashWriter 一边写出一边计算哈希。
type hashWriter struct {
	h hasher
	w io.Writer
}

type hasher interface {
	io.Writer
	Sum(b []byte) []byte
}

func (hw *hashWriter) Write(p []byte) (int, error) {
	if _, err := hw.h.Write(p); err != nil {
		return 0, err
	}
	return hw.w.Write(p)
}

// Meta 返回清单元数据（HEAD / GET 解析 Range 用）。
func (s *Service) Meta(ctx context.Context, oid string) (pgdb.Object, error) {
	return s.db.GetObject(ctx, oid)
}

// List 返回对象清单列表。
func (s *Service) List(ctx context.Context) ([]pgdb.ListEntry, error) {
	return s.db.ListObjects(ctx, 0)
}

func (s *Service) loadObject(ctx context.Context, oid string) (pgdb.Object, map[pgdb.BlockKey]pgdb.BlockChecksum, error) {
	obj, err := s.db.GetObject(ctx, oid)
	if err != nil {
		return pgdb.Object{}, nil, err
	}
	checksums, err := s.db.BlockChecksums(ctx, oid)
	if err != nil {
		return pgdb.Object{}, nil, err
	}
	if obj.Stripes > 0 && len(checksums) != obj.Stripes*s.codec.TotalShards() {
		return pgdb.Object{}, nil, fmt.Errorf("清单不完整: 块校验值 %d 条, 预期 %d 条",
			len(checksums), obj.Stripes*s.codec.TotalShards())
	}
	return obj, checksums, nil
}

// ---------------- 健康检查与修复 ----------------

type ShardHealth struct {
	Node         int  `json:"node"`
	NodeUp       bool `json:"node_up"`
	ShardPresent bool `json:"shard_present"`
	OK           int  `json:"ok_blocks"`
	Corrupt      int  `json:"corrupt_blocks"`
	Missing      int  `json:"missing_blocks"`
	Unavailable  int  `json:"unavailable_blocks"`
}

type HealthReport struct {
	ObjectID string        `json:"object_id"`
	Size     int64         `json:"size"`
	Stripes  int           `json:"stripes"`
	D        int           `json:"data_shards"`
	P        int           `json:"parity_shards"`
	Shards   []ShardHealth `json:"shards"`
	// 每个条带当前有效块数；最小的那个决定可恢复性
	GoodPerStripe []int `json:"good_per_stripe"`
	MinGood       int   `json:"min_good_blocks"`
	Recoverable   bool  `json:"recoverable"`
	Healthy       bool  `json:"healthy"` // 全部块 ok 才为 true
}

// Health 只检测、不改动任何数据。
func (s *Service) Health(ctx context.Context, oid string) (*HealthReport, error) {
	obj, checksums, err := s.loadObject(ctx, oid)
	if err != nil {
		return nil, err
	}
	T := s.codec.TotalShards()
	rep := &HealthReport{
		ObjectID: oid, Size: obj.Size, Stripes: obj.Stripes,
		D: s.codec.DataShards(), P: s.codec.ParityShards(),
		Shards:        make([]ShardHealth, T),
		GoodPerStripe: make([]int, obj.Stripes),
		MinGood:       T,
		Recoverable:   true,
		Healthy:       true,
	}
	for node := 0; node < T; node++ {
		rep.Shards[node] = ShardHealth{Node: node, NodeUp: s.nodes.IsUp(node), ShardPresent: s.nodes.ShardExists(oid, node)}
	}

	for st := 0; st < obj.Stripes; st++ {
		_, infos, _, err := s.readStripe(ctx, obj, checksums, st)
		good := 0
		if err != nil {
			var ue *UnrecoverableError
			if errors.As(err, &ue) {
				good = ue.Good
			}
			rep.Recoverable = false
		} else {
			for _, i := range infos {
				if i.Status == BlockOK {
					good++
				}
			}
		}
		rep.GoodPerStripe[st] = good
		if good < rep.MinGood {
			rep.MinGood = good
		}
		for _, i := range infos {
			sh := &rep.Shards[i.Node]
			switch i.Status {
			case BlockOK:
				sh.OK++
			case BlockCorrupt:
				sh.Corrupt++
				rep.Healthy = false
			case BlockMissing:
				sh.Missing++
				rep.Healthy = false
			case BlockUnavailable:
				sh.Unavailable++
				rep.Healthy = false
			}
		}
	}
	if obj.Stripes == 0 {
		rep.MinGood = 0
		rep.Recoverable = true
		rep.Healthy = true
	}
	return rep, nil
}

type RepairReport struct {
	ObjectID      string              `json:"object_id"`
	Recoverable   bool                `json:"recoverable"`
	Stripes       []StripeRepair      `json:"stripes"`
	Unrecoverable *UnrecoverableError `json:"unrecoverable,omitempty"`
}

type StripeRepair struct {
	Stripe     int   `json:"stripe"`
	Rebuilt    []int `json:"rebuilt_nodes"`
	HadFailure bool  `json:"had_failure"`
}

// Repair 对全部条带执行“重建 → 逐块校验 → 写回在线节点”。
// 下线节点不会被写（等它恢复后再次 Repair 即可）。
func (s *Service) Repair(ctx context.Context, oid string) (*RepairReport, error) {
	obj, checksums, err := s.loadObject(ctx, oid)
	if err != nil {
		return nil, err
	}
	rep := &RepairReport{ObjectID: oid, Recoverable: true}
	if obj.Stripes == 0 {
		return rep, nil
	}

	for st := 0; st < obj.Stripes; st++ {
		logical, infos, full, rerr := s.readStripe(ctx, obj, checksums, st)
		_ = logical
		sr := StripeRepair{Stripe: st}
		if rerr != nil {
			var ue *UnrecoverableError
			if errors.As(rerr, &ue) {
				rep.Recoverable = false
				rep.Unrecoverable = ue
				sr.HadFailure = true
				rep.Stripes = append(rep.Stripes, sr)
				return rep, rerr
			}
			return rep, rerr
		}

		for node, i := range infos {
			if i.Status == BlockOK {
				continue
			}
			sr.HadFailure = true
			// 只写在线节点；节点下线时跳过（块先保留在重建结果里也无意义，不落盘）
			if !s.nodes.IsUp(node) {
				continue
			}
			if err := s.nodes.EnsureShardForRepair(oid, node, obj.Stripes, s.codec.BlockSize()); err != nil {
				continue
			}
			if err := s.nodes.WriteBlock(oid, node, st, s.codec.BlockSize(), full[node]); err != nil {
				continue
			}
			// 写回后再读一次做验证（读-改-写路径自检）
			verify, verr := s.nodes.ReadBlock(oid, node, st, s.codec.BlockSize())
			want := checksums[pgdb.BlockKey{Stripe: st, Node: node}]
			if verr != nil {
				continue
			}
			h := sha256.Sum256(verify)
			if hex.EncodeToString(h[:]) != want.SHA256 {
				continue
			}
			sr.Rebuilt = append(sr.Rebuilt, node)
		}
		rep.Stripes = append(rep.Stripes, sr)
	}
	return rep, nil
}

// ---------------- 故障模拟 API ----------------

func (s *Service) SetNodeDown(node int, reason string) error {
	return s.nodes.SetNodeDown(node, reason)
}
func (s *Service) SetNodeUp(node int) error { return s.nodes.SetNodeUp(node) }

// CorruptBlock 在在线节点上翻转一字节：节点可用、内容损坏。
func (s *Service) CorruptBlock(oid string, node, stripe int) error {
	obj, err := s.db.GetObject(context.Background(), oid)
	if err != nil {
		return err
	}
	if stripe < 0 || stripe >= obj.Stripes {
		return fmt.Errorf("条带 %d 越界(0..%d)", stripe, obj.Stripes-1)
	}
	return s.nodes.CorruptBlock(oid, node, stripe, s.codec.BlockSize())
}

// RemoveShard 删除在线节点上的整份分片（节点在线、分片缺失）。
func (s *Service) RemoveShard(oid string, node int) error {
	return s.nodes.RemoveShard(oid, node)
}

// SortedNodeList 给 API 层用的小工具。
func SortedNodes(nodes []int) string {
	cp := append([]int(nil), nodes...)
	sort.Ints(cp)
	parts := make([]string, len(cp))
	for i, n := range cp {
		parts[i] = fmt.Sprintf("%d", n)
	}
	return strings.Join(parts, ",")
}

// bodyReader 记录请求体读取过程中是否出现过 EOF 以外的错误（客户端断连）。
type bodyReader struct {
	r   io.Reader
	err error
}

func (b *bodyReader) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	if err != nil && err != io.EOF {
		b.err = err
	}
	return n, err
}
