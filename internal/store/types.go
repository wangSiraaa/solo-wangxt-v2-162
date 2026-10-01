// Package store 实现纠删码对象存储的核心逻辑：
// 用 klauspost/reedsolomon 做纠删码，本地多个目录模拟独立节点，
// PostgreSQL 保存对象清单、分片布局和逐块校验值。
package store

import (
	"fmt"
	"time"
)

// ObjectMeta 是 objects 表中一行对象清单。
type ObjectMeta struct {
	ID             string
	Size           int64 // 原始对象字节数
	DataShards     int
	ParityShards   int
	ShardSize      int64  // 每个分片补齐后的统一字节数
	Padding        int64  // 末尾补零字节数，读取时必须截掉
	ChecksumSHA256 string // 原始对象整体 SHA-256
	CreatedAt      time.Time
}

// ShardMeta 是 shards 表中一行分片布局记录。
type ShardMeta struct {
	ObjectID       string
	Index          int // 0..Data-1 为数据分片，其后为校验分片
	Node           int
	Kind           string // "data" | "parity"
	Size           int64
	ChecksumSHA256 string // 分片内容 SHA-256：逐块校验的依据
}

// 分片状态：严格区分“节点不可用/分片丢失”与“分片内容损坏”。
const (
	ShardOK          = "ok"          // 可读且校验一致
	ShardUnavailable = "unavailable" // 节点 down 或分片文件缺失（拿不到字节）
	ShardCorrupt     = "corrupt"     // 字节拿到了，但长度不对或校验值不符
)

// ShardInfo 是检测（status）时单个分片的结果。
type ShardInfo struct {
	Index            int    `json:"index"`
	Node             int    `json:"node"`
	Kind             string `json:"kind"`
	Status           string `json:"status"`
	Size             int64  `json:"size"`
	ExpectedChecksum string `json:"expected_checksum"`
	ActualChecksum   string `json:"actual_checksum,omitempty"`
	Detail           string `json:"detail,omitempty"` // unavailable/corrupt 的具体原因
}

// ObjectStatus 是对象检测结果。
type ObjectStatus struct {
	Object ObjectMeta  `json:"object"`
	Shards []ShardInfo `json:"shards"`

	OKCount       int `json:"ok_count"`
	UnavailableNo int `json:"unavailable_count"`
	CorruptCount  int `json:"corrupt_count"`

	// Healthy：全部分片完好；
	// Recoverable：虽然有坏片/缺片，但有效分片 >= 数据分片数，可重建；
	// 两者皆否则明确不可恢复。
	Healthy     bool `json:"healthy"`
	Recoverable bool `json:"recoverable"`
}

// UnrecoverableError 表示有效分片不足，无法重建。
type UnrecoverableError struct {
	ObjectID      string
	DataShards    int // 重建至少需要的有效分片数
	ValidShards   int // 当前实际有效分片数
	UnavailableNo []int
	CorruptIdx    []int
}

func (e *UnrecoverableError) Error() string {
	return fmt.Sprintf("对象 %s 不可恢复：重建需要 %d 个有效分片，当前仅有 %d 个（不可用分片 %v，损坏分片 %v）",
		e.ObjectID, e.DataShards, e.ValidShards, e.UnavailableNo, e.CorruptIdx)
}

// Is 让 errors.Is(err, ErrUnrecoverable) 成立。
func (e *UnrecoverableError) Is(target error) bool {
	_, ok := target.(*UnrecoverableError)
	return ok
}

// ErrUnrecoverable 配合 errors.Is 使用。
var ErrUnrecoverable = &UnrecoverableError{}

// TestHooks 仅供教学测试模拟上传中断，生产环境不要设置。
type TestHooks struct {
	// FailAfterStaging：所有分片已写入暂存区并自检通过、但尚未改名发布时“崩溃”。
	FailAfterStaging bool
	// FailAfterRename：分片已改到最终位置、但数据库清单事务尚未提交时“崩溃”。
	FailAfterRename bool
}
