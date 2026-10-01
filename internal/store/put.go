package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
)

// PutResult 是上传结果。AlreadyExists 为 true 时表示内容相同的对象已存在（幂等）。
type PutResult struct {
	ID            string `json:"id"`
	Size          int64  `json:"size"`
	Checksum      string `json:"checksum_sha256"`
	DataShards    int    `json:"data_shards"`
	ParityShards  int    `json:"parity_shards"`
	Padding       int64  `json:"padding"`
	AlreadyExists bool   `json:"already_exists"`
}

// ErrUploadAborted 模拟“上传中断”：清单从未发布，对外等于上传失败。
var ErrUploadAborted = errors.New("上传中断：对象清单未发布")

// Put 将一个对象做纠删码编码并写入各节点目录，最后才在数据库中发布清单。
//
// 发布顺序保证“不会出现一份实际无法读取的完整清单”：
//  1. 全部 D+P 个分片先写入各自节点的 staging 暂存目录，回读逐块校验；
//  2. 暂存分片 rename 到最终位置，再次逐块校验（最终路径必须真实可读）；
//  3. 只有上面两步都成功，才在一个数据库事务里写入 objects+shards 清单；
//  4. 事务提交后对象才算发布。中途任一步失败都清理暂存/最终文件，不写清单。
//
// dataShards/parityShards 传 0 表示使用服务默认配置。
func (m *Manager) Put(ctx context.Context, data []byte, dataShards, parityShards int) (*PutResult, error) {
	if dataShards == 0 {
		dataShards = m.defaultD
	}
	if parityShards == 0 {
		parityShards = m.defaultP
	}
	if dataShards < 1 || parityShards < 1 {
		return nil, fmt.Errorf("数据分片数与校验分片数都必须 >= 1")
	}

	sum := sha256.Sum256(data)
	id := hex.EncodeToString(sum[:])

	m.objLocks.Lock(id)
	defer m.objLocks.Unlock(id)

	// 幂等：内容相同（ID 相同）直接返回已有清单。
	if existing, err := m.getObject(ctx, id); err == nil {
		return &PutResult{
			ID: existing.ID, Size: existing.Size, Checksum: existing.ChecksumSHA256,
			DataShards: existing.DataShards, ParityShards: existing.ParityShards,
			Padding: existing.Padding, AlreadyExists: true,
		}, nil
	} else {
		var nf *NotFoundError
		if !errors.As(err, &nf) {
			return nil, fmt.Errorf("检查重复对象失败: %w", err)
		}
	}

	total := dataShards + parityShards

	// 节点不可用属于运行期故障：上传要求全部目标节点在线。
	down, err := m.downNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("读取节点状态失败: %w", err)
	}
	for n := 0; n < total; n++ {
		if down[n] {
			return nil, fmt.Errorf("节点 %d 当前不可用，无法上传（请先恢复该节点）", n)
		}
	}
	// 目标节点必须已登记（启动时按默认 D+P 登记；自定义布局时补登记）。
	if err := m.EnsureNodes(ctx, total); err != nil {
		return nil, err
	}

	size := int64(len(data))
	var shards [][]byte
	var shardSize int64
	var padding int64

	if size == 0 {
		// 空对象：不做编码，也没有任何分片；清单中 shard_size/padding 都为 0。
		shards = nil
		shardSize = 0
		padding = 0
	} else {
		enc, err := m.encoder(dataShards, parityShards)
		if err != nil {
			return nil, err
		}
		// Split 会把数据拷贝进 D 个数据分片并自动补零到等长。
		shards, err = enc.Split(data)
		if err != nil {
			return nil, fmt.Errorf("数据切分失败: %w", err)
		}
		if err := enc.Encode(shards); err != nil {
			return nil, fmt.Errorf("校验分片编码失败: %w", err)
		}
		shardSize = int64(len(shards[0]))
		padding = shardSize*int64(dataShards) - size
	}

	// 记录分片校验值（逐块校验依据）。
	checksums := make([]string, total)
	for i, sh := range shards {
		s := sha256.Sum256(sh)
		checksums[i] = shardHex(s)
	}

	// 第 1 步：写暂存目录并回读校验。
	staged := make([]string, 0, total)
	cleanup := func() {
		for _, p := range staged {
			os.Remove(p)
		}
		for i := 0; i < total; i++ {
			os.Remove(m.finalPath(i, id, i))
		}
	}

	for i, sh := range shards {
		sp := m.stagingPath(i, id, i)
		if err := writeFileAtomic(sp, sh); err != nil {
			cleanup()
			return nil, fmt.Errorf("节点 %d 写暂存分片 %d 失败: %w", i, i, err)
		}
		staged = append(staged, sp)
		if err := verifyFile(sp, int64(len(sh)), checksums[i]); err != nil {
			cleanup()
			return nil, fmt.Errorf("节点 %d 暂存分片 %d 自检失败: %w", i, i, err)
		}
	}

	// 教学测试钩子：模拟“暂存完成后进程崩溃”。此时没有最终文件、没有清单。
	if m.TestHooks != nil && m.TestHooks.FailAfterStaging {
		cleanup()
		return nil, ErrUploadAborted
	}

	// 第 2 步：暂存文件 rename 到最终位置，随后再次逐块校验。
	for i := range shards {
		fp := m.finalPath(i, id, i)
		if err := os.MkdirAll(m.objectDir(i, id), 0o755); err != nil {
			cleanup()
			return nil, err
		}
		if err := os.Rename(staged[i], fp); err != nil {
			cleanup()
			return nil, fmt.Errorf("节点 %d 发布分片 %d 失败: %w", i, i, err)
		}
		if err := verifyFile(fp, shardSize, checksums[i]); err != nil {
			cleanup()
			return nil, fmt.Errorf("节点 %d 最终分片 %d 校验失败，放弃发布清单: %w", i, i, err)
		}
	}

	// 教学测试钩子：模拟“分片落盘后、数据库提交前崩溃”。
	// 此时留下的是无清单的孤儿分片，读取端因查无清单返回 404，绝不会当成完整对象。
	if m.TestHooks != nil && m.TestHooks.FailAfterRename {
		return nil, ErrUploadAborted
	}

	// 第 3 步：事务发布清单。只有提交成功对象才真正存在。
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("开启清单事务失败: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx,
		`INSERT INTO objects(id, size, data_shards, parity_shards, shard_size, padding, checksum_sha256, status)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,'complete')`,
		id, size, dataShards, parityShards, shardSize, padding, id)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("写入对象清单失败: %w", err)
	}
	for i := 0; i < total; i++ {
		kind := "data"
		if i >= dataShards {
			kind = "parity"
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO shards(object_id, shard_index, node_id, kind, size, checksum_sha256)
			 VALUES ($1,$2,$3,$4,$5,$6)`,
			id, i, i, kind, shardSize, checksums[i]); err != nil {
			cleanup()
			return nil, fmt.Errorf("写入分片布局 %d 失败: %w", i, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		// 事务未提交成功：清单对外不可见，删除分片避免“有文件无清单”的困惑。
		cleanup()
		return nil, fmt.Errorf("提交清单事务失败（对象未发布）: %w", err)
	}

	return &PutResult{
		ID: id, Size: size, Checksum: hex.EncodeToString(sum[:]),
		DataShards: dataShards, ParityShards: parityShards, Padding: padding,
	}, nil
}

// verifyFile 读取文件并比对长度与 SHA-256，长度不符或摘要不符即为损坏。
func verifyFile(path string, wantSize int64, wantSHA string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if int64(len(b)) != wantSize {
		return fmt.Errorf("分片长度不符: 期望 %d，实际 %d", wantSize, len(b))
	}
	s := sha256.Sum256(b)
	if got := hex.EncodeToString(s[:]); got != wantSHA {
		return fmt.Errorf("分片校验值不符: 期望 %s，实际 %s", wantSHA, got)
	}
	return nil
}
