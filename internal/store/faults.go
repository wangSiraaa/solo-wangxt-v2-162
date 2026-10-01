package store

import (
	"context"
	"fmt"
	"os"
)

// NodeState 节点状态行。
type NodeState struct {
	Node  int    `json:"node"`
	State string `json:"state"` // up / down
}

// ListNodes 返回全部已登记节点及其模拟状态。
func (m *Manager) ListNodes(ctx context.Context) ([]NodeState, error) {
	states, err := m.allNodeStates(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]NodeState, 0, len(states))
	for n, s := range states {
		out = append(out, NodeState{Node: n, State: s})
	}
	// map 遍历无序，按编号排一下
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j].Node < out[i].Node {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out, nil
}

// SetNodeDown / SetNodeUp 模拟节点不可用与恢复。
// down 只改状态：目录和文件原封不动，恢复 up 后分片立即可读，
// 这与“分片内容损坏”是两类完全不同的故障。
func (m *Manager) SetNodeDown(ctx context.Context, node int) error {
	return m.setNodeState(ctx, node, "down")
}

func (m *Manager) SetNodeUp(ctx context.Context, node int) error {
	return m.setNodeState(ctx, node, "up")
}

func (m *Manager) setNodeState(ctx context.Context, node int, state string) error {
	tag, err := m.pool.Exec(ctx,
		`UPDATE nodes SET state = $2, updated_at = now() WHERE node_id = $1`, node, state)
	if err != nil {
		return fmt.Errorf("更新节点 %d 状态失败: %w", node, err)
	}
	if tag.RowsAffected() == 0 {
		return &NotFoundError{ID: fmt.Sprintf("node-%d", node)}
	}
	return nil
}

// CorruptShard 模拟“分片内容损坏”：在节点仍然在线、文件仍然存在的前提下，
// 截断或翻转文件字节。读取端能拿到字节，但 SHA-256 与清单不符 -> corrupt。
//
// mode: "flip"（翻转首字节，保持长度）或 "truncate"（截断，长度不符）。
func (m *Manager) CorruptShard(ctx context.Context, id string, shardIdx int, mode string) (actualSHA string, err error) {
	sm, err := m.locateShard(ctx, id, shardIdx)
	if err != nil {
		return "", err
	}
	down, err := m.downNodes(ctx)
	if err != nil {
		return "", err
	}
	if down[sm.Node] {
		return "", fmt.Errorf("节点 %d 处于不可用状态：内容损坏故障只能在节点在线时注入（两类故障不能混为一谈）", sm.Node)
	}
	path := m.finalPath(sm.Node, id, shardIdx)
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("读取待损坏分片失败: %w", err)
	}

	m.objLocks.Lock(id)
	defer m.objLocks.Unlock(id)

	switch mode {
	case "flip":
		if len(b) == 0 {
			// 空对象没有分片，不会走到这里；保险起见
			return "", fmt.Errorf("空分片无法翻转")
		}
		b[0] ^= 0xFF
	case "truncate":
		if len(b) > 1 {
			b = b[:len(b)-1]
		}
	case "garbage":
		// 整体替换为等长的零字节（保持长度，仅内容错），内容全零时也与原校验值不同
		for i := range b {
			b[i] = 0
		}
	default:
		return "", fmt.Errorf("未知损坏模式 %q（支持 flip / truncate / garbage）", mode)
	}
	if err := writeFileAtomic(path, b); err != nil {
		return "", fmt.Errorf("写入损坏分片失败: %w", err)
	}
	return sha256Hex(b), nil
}

// DeleteShard 模拟节点在线但分片文件丢失（区别于节点整机不可用）：
// 读取端拿不到字节 -> unavailable，但节点本身可服务其他分片。
func (m *Manager) DeleteShard(ctx context.Context, id string, shardIdx int) error {
	sm, err := m.locateShard(ctx, id, shardIdx)
	if err != nil {
		return err
	}
	m.objLocks.Lock(id)
	defer m.objLocks.Unlock(id)
	if err := os.Remove(m.finalPath(sm.Node, id, shardIdx)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("删除分片失败: %w", err)
	}
	return nil
}

// locateShard 找到指定分片的布局记录，并校验编号范围。
func (m *Manager) locateShard(ctx context.Context, id string, shardIdx int) (ShardMeta, error) {
	if _, err := m.getObject(ctx, id); err != nil {
		return ShardMeta{}, err
	}
	layout, err := m.listShards(ctx, id)
	if err != nil {
		return ShardMeta{}, fmt.Errorf("读取分片布局失败: %w", err)
	}
	if shardIdx < 0 || shardIdx >= len(layout) {
		return ShardMeta{}, fmt.Errorf("分片编号 %d 越界（对象 %s 共 %d 片）", shardIdx, id, len(layout))
	}
	return layout[shardIdx], nil
}

// Repair 显式触发重建修复：达到恢复条件（有效分片 >= D）时逐块校验并回写；
// 不足则返回 UnrecoverableError，明确说明不可恢复。
func (m *Manager) Repair(ctx context.Context, id string) (*ObjectStatus, error) {
	meta, err := m.getObject(ctx, id)
	if err != nil {
		return nil, err
	}
	m.objLocks.Lock(id)
	defer m.objLocks.Unlock(id)

	layout, err := m.listShards(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("读取分片布局失败: %w", err)
	}
	if _, _, err := m.assemble(ctx, meta, layout, true); err != nil {
		// 不可恢复：返回错误，同时让调用方能看到当前各分片状态。
		if ue, ok := err.(*UnrecoverableError); ok {
			return nil, ue
		}
		return nil, err
	}
	// 重建回写后重新检测，确认修复结果逐块可读、校验一致。
	return m.Status(ctx, id)
}

// DeleteObject 删除对象清单（事务提交后分片文件成为无主数据）并删除各节点文件。
// 主要供测试清理使用。
func (m *Manager) DeleteObject(ctx context.Context, id string) error {
	if _, err := m.getObject(ctx, id); err != nil {
		return err
	}
	m.objLocks.Lock(id)
	defer m.objLocks.Unlock(id)

	layout, err := m.listShards(ctx, id)
	if err != nil {
		return err
	}
	if _, err := m.pool.Exec(ctx, `DELETE FROM objects WHERE id = $1`, id); err != nil {
		return fmt.Errorf("删除清单失败: %w", err)
	}
	for _, sm := range layout {
		// 教学环境里文件系统是共享的，即使节点 down 也能删。
		_ = os.Remove(m.finalPath(sm.Node, id, sm.Index))
	}
	return nil
}

// ListObjects 列出对象 ID 与大小（供教学观察/测试使用）。
func (m *Manager) ListObjects(ctx context.Context) ([]ObjectMeta, error) {
	rows, err := m.pool.Query(ctx,
		`SELECT id, size, data_shards, parity_shards, shard_size, padding, checksum_sha256, created_at
		 FROM objects ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ObjectMeta
	for rows.Next() {
		var o ObjectMeta
		if err := rows.Scan(&o.ID, &o.Size, &o.DataShards, &o.ParityShards,
			&o.ShardSize, &o.Padding, &o.ChecksumSHA256, &o.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}
