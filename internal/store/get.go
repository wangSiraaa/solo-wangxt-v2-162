package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
)

// NotFoundError 对象或分片资源不存在（清单中没有该对象）。
type NotFoundError struct{ ID string }

func (e *NotFoundError) Error() string   { return fmt.Sprintf("对象 %s 不存在", e.ID) }
func (e *NotFoundError) Is(t error) bool { _, ok := t.(*NotFoundError); return ok }

// ErrNotFound 配合 errors.Is 使用。
var ErrNotFound = &NotFoundError{}

// getObject 读取对象清单；不存在时返回包装了 ErrNotFound 的错误。
func (m *Manager) getObject(ctx context.Context, id string) (*ObjectMeta, error) {
	var o ObjectMeta
	err := m.pool.QueryRow(ctx,
		`SELECT id, size, data_shards, parity_shards, shard_size, padding, checksum_sha256, created_at
		 FROM objects WHERE id = $1`, id).
		Scan(&o.ID, &o.Size, &o.DataShards, &o.ParityShards, &o.ShardSize, &o.Padding, &o.ChecksumSHA256, &o.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, &NotFoundError{ID: id}
	}
	if err != nil {
		return nil, err
	}
	return &o, nil
}

// listShards 读取分片布局，按 shard_index 排序。
func (m *Manager) listShards(ctx context.Context, id string) ([]ShardMeta, error) {
	rows, err := m.pool.Query(ctx,
		`SELECT object_id, shard_index, node_id, kind, size, checksum_sha256
		 FROM shards WHERE object_id = $1 ORDER BY shard_index`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ShardMeta
	for rows.Next() {
		var s ShardMeta
		if err := rows.Scan(&s.ObjectID, &s.Index, &s.Node, &s.Kind, &s.Size, &s.ChecksumSHA256); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// Read 读取对象的 [start, start+length) 区间。length<0 表示读到末尾。
//
// 读取流程：
//  1. 查清单与分片布局（无清单直接 NotFound，孤儿分片永远不会被当成对象）；
//  2. 逐块读取并校验分片，严格区分不可用与损坏；
//  3. 有效分片 >= D 时用 Reed-Solomon 重建缺失分片，< D 明确返回不可恢复；
//  4. 拼出完整对象，截掉清单记录的尾部补齐零，并核对整体 SHA-256；
//  5. 截出请求区间返回；成功重建的分片会回写到对应节点完成修复。
func (m *Manager) Read(ctx context.Context, id string, start, length int64) ([]byte, error) {
	meta, err := m.getObject(ctx, id)
	if err != nil {
		return nil, err
	}
	if start < 0 || start > meta.Size {
		return nil, fmt.Errorf("范围起点 %d 越界（对象大小 %d）", start, meta.Size)
	}
	end := meta.Size
	if length >= 0 {
		end = start + length
		if end > meta.Size {
			end = meta.Size // 超出尾部按实际长度返回，与 HTTP Range 习惯一致
		}
	}

	if meta.Size == 0 {
		// 空对象没有分片，直接返回空内容。
		return []byte{}, nil
	}

	layout, err := m.listShards(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("读取分片布局失败: %w", err)
	}

	shards, _, err := m.assemble(ctx, meta, layout, true)
	if err != nil {
		return nil, err
	}

	full := joinObject(shards, meta.DataShards, meta.Size)
	// 整体校验：确保补齐零没有被当成原文件内容（joinObject 已按 size 截断，
	// 这里再用摘要确认重建结果本身正确）。
	sum := sha256.Sum256(full)
	if got := hex.EncodeToString(sum[:]); got != meta.ChecksumSHA256 {
		return nil, fmt.Errorf("重建后对象整体校验失败：期望 %s，实际 %s（数据可能已损坏到超出纠删码能力）",
			meta.ChecksumSHA256, got)
	}
	return full[start:end], nil
}

// shardBlock 表示一次读取中单个分片的实际情况。
type shardBlock struct {
	meta   ShardMeta
	data   []byte // 仅在 status==ok 时有效
	status string
	detail string
}

// assemble 逐块读取分片，必要时重建，并将修复结果回写。
// 返回的 shards 一定是全部等长（或调用者处理空对象）。
func (m *Manager) assemble(ctx context.Context, meta *ObjectMeta, layout []ShardMeta, repair bool) ([][]byte, []shardBlock, error) {
	total := meta.DataShards + meta.ParityShards
	if len(layout) != total {
		return nil, nil, fmt.Errorf("分片布局不完整：清单记录 %d 片，应为 %d 片", len(layout), total)
	}

	down, err := m.downNodes(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("读取节点状态失败: %w", err)
	}

	blocks := make([]shardBlock, total)
	valid := 0
	for _, sm := range layout {
		b := shardBlock{meta: sm, status: ShardUnavailable}
		path := m.finalPath(sm.Node, meta.ID, sm.Index)
		if down[sm.Node] {
			b.status = ShardUnavailable
			b.detail = fmt.Sprintf("节点 %d 不可用", sm.Node)
		} else {
			data, rerr := os.ReadFile(path)
			if rerr != nil {
				if os.IsNotExist(rerr) {
					b.status = ShardUnavailable
					b.detail = fmt.Sprintf("节点 %d 上分片文件缺失: %s", sm.Node, path)
				} else {
					b.status = ShardUnavailable
					b.detail = fmt.Sprintf("节点 %d 分片读取失败: %v", sm.Node, rerr)
				}
			} else if int64(len(data)) != sm.Size {
				b.status = ShardCorrupt
				b.detail = fmt.Sprintf("分片长度损坏: 期望 %d，实际 %d", sm.Size, len(data))
				b.data = data
			} else {
				sum := sha256.Sum256(data)
				if hex.EncodeToString(sum[:]) != sm.ChecksumSHA256 {
					b.status = ShardCorrupt
					b.detail = "分片内容校验值不符（内容损坏）"
					b.data = data
				} else {
					b.status = ShardOK
					b.data = data
					valid++
				}
			}
		}
		blocks[sm.Index] = b
	}

	shards := make([][]byte, total)
	for i := range blocks {
		if blocks[i].status == ShardOK {
			shards[i] = blocks[i].data
		}
	}

	if valid < meta.DataShards {
		bad := &UnrecoverableError{
			ObjectID:    meta.ID,
			DataShards:  meta.DataShards,
			ValidShards: valid,
		}
		for i, b := range blocks {
			switch b.status {
			case ShardUnavailable:
				bad.UnavailableNo = append(bad.UnavailableNo, i)
			case ShardCorrupt:
				bad.CorruptIdx = append(bad.CorruptIdx, i)
			}
		}
		return nil, blocks, bad
	}

	needRepair := valid < total
	if needRepair {
		enc, err := m.encoder(meta.DataShards, meta.ParityShards)
		if err != nil {
			return nil, blocks, err
		}
		// ReconstructData 只补数据分片即可校验整体对象；
		// 但教学项目希望“重建并逐块校验”，所以用 Reconstruct 补全所有分片。
		if err := enc.Reconstruct(shards); err != nil {
			return nil, blocks, fmt.Errorf("纠删码重建失败: %w", err)
		}
		// 逐块校验重建结果：每个重建分片都必须与清单记录的校验值一致。
		for i, b := range blocks {
			if b.status == ShardOK {
				continue
			}
			sum := sha256.Sum256(shards[i])
			if got := hex.EncodeToString(sum[:]); got != b.meta.ChecksumSHA256 {
				return nil, blocks, fmt.Errorf("分片 %d 重建结果校验失败：期望 %s，实际 %s",
					i, b.meta.ChecksumSHA256, got)
			}
		}
		if repair {
			if err := m.writeRepairs(ctx, meta, blocks, shards, down); err != nil {
				// 回写失败不影响本次读取，但要让调用者知道节点仍未修复。
				fmt.Fprintf(os.Stderr, "[warn] 对象 %s 部分分片重建校验通过但回写失败: %v\n", meta.ID, err)
			}
		}
	}

	return shards, blocks, nil
}

// writeRepairs 把重建出的坏片/缺片原子地写回其所在节点（节点仍 down 时跳过）。
func (m *Manager) writeRepairs(ctx context.Context, meta *ObjectMeta, blocks []shardBlock, shards [][]byte, down map[int]bool) error {
	var firstErr error
	for i, b := range blocks {
		if b.status == ShardOK {
			continue
		}
		if down[b.meta.Node] {
			// 节点不可用时无法回写；等节点恢复后再次读取/修复即可。
			continue
		}
		fp := m.finalPath(b.meta.Node, meta.ID, i)
		if err := writeFileAtomic(fp, shards[i]); err != nil && firstErr == nil {
			firstErr = err
			continue
		}
		if err := verifyFile(fp, b.meta.Size, b.meta.ChecksumSHA256); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
