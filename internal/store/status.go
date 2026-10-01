package store

import (
	"context"
	"fmt"
)

// Status 逐块检测对象的全部分片，不做任何修复。
// 节点不可用与分片内容损坏分别计数，并给出是否达到恢复条件的结论。
func (m *Manager) Status(ctx context.Context, id string) (*ObjectStatus, error) {
	meta, err := m.getObject(ctx, id)
	if err != nil {
		return nil, err
	}
	layout, err := m.listShards(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("读取分片布局失败: %w", err)
	}

	st := &ObjectStatus{Object: *meta, Shards: make([]ShardInfo, 0, len(layout))}

	if meta.Size == 0 {
		// 空对象无分片，天然健康（容错无从谈起）。
		st.Healthy = true
		st.Recoverable = true
		return st, nil
	}

	_, blocks, err := m.assemble(ctx, meta, layout, false)
	if err != nil {
		// 不可恢复也要把检测结果返回给调用方，错误单独带出。
		if ue, ok := err.(*UnrecoverableError); ok {
			st.Recoverable = false
			st.Healthy = false
			fillStatusFromBlocks(st, blocks)
			return st, ue
		}
		return nil, err
	}

	fillStatusFromBlocks(st, blocks)
	st.Healthy = st.OKCount == meta.DataShards+meta.ParityShards
	st.Recoverable = st.OKCount >= meta.DataShards
	return st, nil
}

func fillStatusFromBlocks(st *ObjectStatus, blocks []shardBlock) {
	for _, b := range blocks {
		info := ShardInfo{
			Index: b.meta.Index, Node: b.meta.Node, Kind: b.meta.Kind,
			Status: b.status, Size: b.meta.Size,
			ExpectedChecksum: b.meta.ChecksumSHA256,
			Detail:           b.detail,
		}
		if b.status == ShardCorrupt && b.data != nil {
			info.ActualChecksum = sha256Hex(b.data)
		}
		st.Shards = append(st.Shards, info)
		switch b.status {
		case ShardOK:
			st.OKCount++
		case ShardUnavailable:
			st.UnavailableNo++
		case ShardCorrupt:
			st.CorruptCount++
		}
	}
}
