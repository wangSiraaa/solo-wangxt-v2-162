package nodestore_test

import (
	"errors"
	"testing"

	"ecstore/internal/nodestore"
)

func TestDownVsCorruptAreDistinct(t *testing.T) {
	dir := t.TempDir()
	s, err := nodestore.New(dir, 3)
	if err != nil {
		t.Fatal(err)
	}
	oid := "obj-a"
	block := make([]byte, 32)
	for i := range block {
		block[i] = byte('A' + i%26)
	}

	// 三个节点各写一块并提交
	for n := 0; n < 3; n++ {
		if err := s.AppendStaging(oid, n, 0, block); err != nil {
			t.Fatalf("append %d: %v", n, err)
		}
		if err := s.CommitStaging(oid, n); err != nil {
			t.Fatalf("commit %d: %v", n, err)
		}
	}

	// 正常读
	if _, err := s.ReadBlock(oid, 1, 0, 32); err != nil {
		t.Fatalf("正常读取失败: %v", err)
	}

	// 节点 1 下线 -> ErrNodeDown（不是内容损坏）
	if err := s.SetNodeDown(1, "test"); err != nil {
		t.Fatal(err)
	}
	if s.IsUp(1) {
		t.Fatal("节点应处于下线状态")
	}
	if _, err := s.ReadBlock(oid, 1, 0, 32); !errors.Is(err, nodestore.ErrNodeDown) {
		t.Fatalf("下线节点读取应返回 ErrNodeDown, got %v", err)
	}
	if err := s.SetNodeUp(1); err != nil {
		t.Fatal(err)
	}

	// 节点 2 保持在线，但分片被删 -> ErrShardMissing（节点可用、内容没了）
	if err := s.RemoveShard(oid, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadBlock(oid, 2, 0, 32); !errors.Is(err, nodestore.ErrNodeDown) && !errors.Is(err, nodestore.ErrShardMissing) {
		t.Fatalf("应返回 ErrShardMissing/ErrNodeDown, got %v", err)
	} else if !errors.Is(err, nodestore.ErrShardMissing) {
		t.Fatalf("应精确返回 ErrShardMissing, got %v", err)
	}

	// 内容损坏：节点在线、文件可读（校验差异由上层判断，这里验证能读出且字节变了）
	if err := s.CorruptBlock(oid, 0, 0, 32); err != nil {
		t.Fatal(err)
	}
	raw, err := s.ReadBlock(oid, 0, 0, 32)
	if err != nil {
		t.Fatalf("损坏块应当仍能读取: %v", err)
	}
	if string(raw) == string(block) {
		t.Fatal("翻转后内容应发生变化")
	}
}

func TestStagingCleanupAndAbort(t *testing.T) {
	dir := t.TempDir()
	s, err := nodestore.New(dir, 3)
	if err != nil {
		t.Fatal(err)
	}
	oid := "obj-b"
	block := make([]byte, 16)
	for n := 0; n < 3; n++ {
		if err := s.AppendStaging(oid, n, 0, block); err != nil {
			t.Fatal(err)
		}
	}
	// 中断上传：临时分片全部清掉，且没有正式分片
	s.AbortStaging(oid)
	if s.ShardExists(oid, 0) {
		t.Fatal("abort 后不应存在正式分片")
	}
	// 重新建库（模拟重启）会清理遗留 tmp
	for n := 0; n < 3; n++ {
		if err := s.AppendStaging("leftover", n, 0, block); err != nil {
			t.Fatal(err)
		}
	}
	s2, err := nodestore.New(dir, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.ReserveStaging("leftover"); err != nil {
		t.Fatalf("重启清理后应可重新预留同名上传, got %v", err)
	}
}

func TestOutOfOrderStripeRejected(t *testing.T) {
	dir := t.TempDir()
	s, _ := nodestore.New(dir, 2)
	b := make([]byte, 8)
	if err := s.AppendStaging("o", 0, 1, b); err == nil {
		t.Fatal("条带 1 先于条带 0 写入应被拒绝")
	}
}

func TestInvalidObjectID(t *testing.T) {
	dir := t.TempDir()
	s, _ := nodestore.New(dir, 2)
	b := make([]byte, 8)
	for _, id := range []string{"../escape", "a/b", "a\\b", ""} {
		if err := s.AppendStaging(id, 0, 0, b); !errors.Is(err, nodestore.ErrInvalidObject) {
			t.Errorf("id=%q 应报 ErrInvalidObject, got %v", id, err)
		}
	}
}

func TestWriteBlockRepair(t *testing.T) {
	dir := t.TempDir()
	s, _ := nodestore.New(dir, 2)
	oid := "r"
	b := make([]byte, 16)
	for i := range b {
		b[i] = byte(i)
	}
	for n := 0; n < 2; n++ {
		_ = s.AppendStaging(oid, n, 0, b)
		_ = s.CommitStaging(oid, n)
	}
	_ = s.RemoveShard(oid, 1)
	if err := s.EnsureShardForRepair(oid, 1, 1, 16); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteBlock(oid, 1, 0, 16, b); err != nil {
		t.Fatal(err)
	}
	got, err := s.ReadBlock(oid, 1, 0, 16)
	if err != nil || string(got) != string(b) {
		t.Fatalf("回写后读取不一致: %v", err)
	}
}
