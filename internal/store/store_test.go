package store_test

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"erasure-store/internal/store"
)

// deterministicData 生成可复现的伪随机数据（不用 math/rand 的全局状态）。
func deterministicData(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i)*31 + seed
	}
	return b
}

// 1. 常规往返：编码 -> 落盘 -> 读回，内容与摘要完全一致。
func TestPutGet_RoundTrip(t *testing.T) {
	mgr, _, ctx := newTestManager(t, 4, 2)
	data := deterministicData(10000, 7)

	res, err := mgr.Put(ctx, data, 0, 0)
	if err != nil {
		t.Fatalf("Put 失败: %v", err)
	}
	if res.Padding != 0 {
		t.Fatalf("10000 字节在 4 分片下应能整除（每片 2500），实际 padding=%d", res.Padding)
	}
	wantSum := sha256.Sum256(data)
	if res.ID != hex.EncodeToString(wantSum[:]) {
		t.Fatalf("对象 ID 应为内容 SHA-256")
	}

	got, err := mgr.Read(ctx, res.ID, 0, -1)
	if err != nil {
		t.Fatalf("Read 失败: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("读回内容与原文不一致（长度 %d vs %d）", len(got), len(data))
	}

	// 初始状态必须健康：6 片全部 ok。
	st, err := mgr.Status(ctx, res.ID)
	if err != nil {
		t.Fatalf("Status 失败: %v", err)
	}
	if !st.Healthy || st.OKCount != 6 || st.CorruptCount != 0 || st.UnavailableNo != 0 {
		t.Fatalf("初始状态应为全健康，实际: %+v", st)
	}
}

// 2. 空对象：没有分片，清单仍可发布且读回长度为 0。
func TestEmptyObject(t *testing.T) {
	mgr, _, ctx := newTestManager(t, 4, 2)

	res, err := mgr.Put(ctx, []byte{}, 0, 0)
	if err != nil {
		t.Fatalf("空对象 Put 失败: %v", err)
	}
	if res.Size != 0 || res.Padding != 0 {
		t.Fatalf("空对象 size/padding 应为 0，实际 size=%d padding=%d", res.Size, res.Padding)
	}

	got, err := mgr.Read(ctx, res.ID, 0, -1)
	if err != nil {
		t.Fatalf("空对象 Read 失败: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("空对象读回必须长度为 0，实际 %d 字节: %x", len(got), got)
	}

	// 空对象没有分片行。
	layout, err := mgr.ListObjects(ctx)
	if err != nil {
		t.Fatalf("ListObjects 失败: %v", err)
	}
	if len(layout) != 1 || layout[0].Size != 0 {
		t.Fatalf("清单里应有且仅有一个空对象，实际 %+v", layout)
	}

	st, err := mgr.Status(ctx, res.ID)
	if err != nil {
		t.Fatalf("空对象 Status 失败: %v", err)
	}
	if !st.Healthy {
		t.Fatalf("空对象应视为健康: %+v", st)
	}
}

// 3. 非整块长度：末尾补齐长度必须记录，且补零绝不能被当成原文件内容返回。
//
//	4003 字节 / 4 数据分片 => 每片 1001，补齐 1 字节。
//	特别让原文最后 3 个字节就是真正的 0x00，检验“合法尾零保留、补齐零剔除”。
func TestNonAlignedLength_PaddingRecordedAndStripped(t *testing.T) {
	mgr, _, ctx := newTestManager(t, 4, 2)
	data := deterministicData(4003, 3)
	data[4000], data[4001], data[4002] = 0, 0, 0 // 真实内容的尾部零

	res, err := mgr.Put(ctx, data, 0, 0)
	if err != nil {
		t.Fatalf("Put 失败: %v", err)
	}
	if res.Padding != 1 {
		t.Fatalf("4003 字节 / 4 片应补 1 字节，实际 padding=%d", res.Padding)
	}
	if res.Size != 4003 {
		t.Fatalf("记录的原始大小应为 4003，实际 %d", res.Size)
	}

	got, err := mgr.Read(ctx, res.ID, 0, -1)
	if err != nil {
		t.Fatalf("Read 失败: %v", err)
	}
	if len(got) != 4003 {
		t.Fatalf("读回长度必须是原始长度 4003（补齐零已剔除），实际 %d", len(got))
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("读回内容与原文不一致（特别检查尾部真实零字节）")
	}

	// 读到对象末尾的范围也不能多带补齐零。
	tail, err := mgr.Read(ctx, res.ID, 4000, -1)
	if err != nil {
		t.Fatalf("尾部范围读取失败: %v", err)
	}
	if len(tail) != 3 || !bytes.Equal(tail, []byte{0, 0, 0}) {
		t.Fatalf("尾部 3 个真实零字节读取错误，得到 %d 字节: %x", len(tail), tail)
	}
}

// 4. 范围读取：任意起点/长度、越过尾部截断、起点恰好等于长度。
func TestRangeRead(t *testing.T) {
	mgr, _, ctx := newTestManager(t, 4, 2)
	data := deterministicData(5000, 11)
	res, err := mgr.Put(ctx, data, 0, 0)
	if err != nil {
		t.Fatalf("Put 失败: %v", err)
	}

	cases := []struct {
		start, length int64
		want          []byte
	}{
		{0, 100, data[0:100]},
		{1234, 10, data[1234:1244]},
		{4990, 1000, data[4990:]}, // 越过尾部 -> 截到真实长度
		{5000, 5, data[5000:]},    // 起点恰好 EOF -> 空
		{2500, -1, data[2500:]},   // length=-1 读到末尾
	}
	for i, c := range cases {
		got, err := mgr.Read(ctx, res.ID, c.start, c.length)
		if err != nil {
			t.Fatalf("用例 %d 读取失败: %v", i, err)
		}
		if !bytes.Equal(got, c.want) {
			t.Fatalf("用例 %d 范围结果不符：得到 %d 字节，期望 %d 字节", i, len(got), len(c.want))
		}
	}

	// 越界起点必须报错。
	if _, err := mgr.Read(ctx, res.ID, 5001, 1); err == nil {
		t.Fatalf("起点越过对象尾部应当报错")
	}
}

// 5. 节点不可用（区别于内容损坏）：down 掉 2 个节点（== 校验分片数）仍可读取并重建。
func TestNodeDown_RecoverableAndAutoRepair(t *testing.T) {
	mgr, _, ctx := newTestManager(t, 4, 2)
	data := deterministicData(8000, 5)
	res, err := mgr.Put(ctx, data, 0, 0)
	if err != nil {
		t.Fatalf("Put 失败: %v", err)
	}

	if err := mgr.SetNodeDown(ctx, 4); err != nil {
		t.Fatal(err)
	}
	if err := mgr.SetNodeDown(ctx, 5); err != nil {
		t.Fatal(err)
	}

	st, err := mgr.Status(ctx, res.ID)
	if err != nil {
		t.Fatalf("Status 失败: %v", err)
	}
	if st.UnavailableNo != 2 || st.CorruptCount != 0 || !st.Recoverable || st.Healthy {
		t.Fatalf("2 个节点 down：不可用=2、损坏=0、可恢复，实际 %+v", st)
	}
	for _, sh := range st.Shards {
		if sh.Node == 4 || sh.Node == 5 {
			if sh.Status != store.ShardUnavailable {
				t.Fatalf("节点 %d 上分片应判为 unavailable，实际 %s", sh.Node, sh.Status)
			}
		}
	}

	// 读取触发重建：down 节点无法回写，但本次读取必须成功且内容正确。
	got, err := mgr.Read(ctx, res.ID, 0, -1)
	if err != nil {
		t.Fatalf("缺少 2 片（恰达容错上限）时应可读取，实际: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("重建后内容不符")
	}

	// 节点恢复后显式 Repair，分片回写，状态回到全健康。
	if err := mgr.SetNodeUp(ctx, 4); err != nil {
		t.Fatal(err)
	}
	if err := mgr.SetNodeUp(ctx, 5); err != nil {
		t.Fatal(err)
	}
	st2, err := mgr.Repair(ctx, res.ID)
	if err != nil {
		t.Fatalf("Repair 失败: %v", err)
	}
	if !st2.Healthy {
		t.Fatalf("修复后应全健康，实际 %+v", st2)
	}
}

// 6. 分片内容损坏（节点在线、文件可读但校验值不符）：2 片损坏仍可逐块校验重建。
func TestShardCorruption_RecoverableAndRebuildVerified(t *testing.T) {
	mgr, _, ctx := newTestManager(t, 4, 2)
	data := deterministicData(8000, 9)
	res, err := mgr.Put(ctx, data, 0, 0)
	if err != nil {
		t.Fatalf("Put 失败: %v", err)
	}

	if _, err := mgr.CorruptShard(ctx, res.ID, 0, "flip"); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.CorruptShard(ctx, res.ID, 5, "truncate"); err != nil {
		t.Fatal(err)
	}

	st, err := mgr.Status(ctx, res.ID)
	if err != nil {
		t.Fatalf("Status 失败: %v", err)
	}
	if st.CorruptCount != 2 || st.UnavailableNo != 0 || !st.Recoverable {
		t.Fatalf("应识别出 2 片内容损坏且可恢复，实际 %+v", st)
	}

	// Read 会重建并逐块校验，且回写修复后的分片。
	got, err := mgr.Read(ctx, res.ID, 0, -1)
	if err != nil {
		t.Fatalf("损坏 2 片时应可重建读取: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("重建内容与原文不符")
	}
	st2, err := mgr.Status(ctx, res.ID)
	if err != nil {
		t.Fatalf("二次 Status 失败: %v", err)
	}
	if !st2.Healthy {
		t.Fatalf("回写修复后应全健康，实际 %+v", st2)
	}
}

// 7. 混合故障未超过容错数：1 节点 down + 1 片内容损坏（共 2 == P），可恢复。
func TestMixedFaults_WithinTolerance(t *testing.T) {
	mgr, _, ctx := newTestManager(t, 4, 2)
	data := deterministicData(8000, 13)
	res, err := mgr.Put(ctx, data, 0, 0)
	if err != nil {
		t.Fatalf("Put 失败: %v", err)
	}

	if err := mgr.SetNodeDown(ctx, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.CorruptShard(ctx, res.ID, 0, "garbage"); err != nil {
		t.Fatal(err)
	}

	st, err := mgr.Status(ctx, res.ID)
	if err != nil {
		t.Fatalf("Status 失败: %v", err)
	}
	if st.UnavailableNo != 1 || st.CorruptCount != 1 || !st.Recoverable {
		t.Fatalf("1 不可用 + 1 损坏应可恢复，实际 %+v", st)
	}
	got, err := mgr.Read(ctx, res.ID, 0, -1)
	if err != nil {
		t.Fatalf("混合故障在容错内应可读取: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("重建内容错误")
	}
}

// 8. 恰好超过容错数量：P+1=3 片无效（2 个节点不可用 + 1 片内容损坏），
//
//	有效分片只剩 3 < 4，必须明确报告不可恢复。
func TestUnrecoverable_ExactlyOverTolerance_Mixed(t *testing.T) {
	mgr, _, ctx := newTestManager(t, 4, 2)
	data := deterministicData(8000, 17)
	res, err := mgr.Put(ctx, data, 0, 0)
	if err != nil {
		t.Fatalf("Put 失败: %v", err)
	}

	if err := mgr.SetNodeDown(ctx, 4); err != nil {
		t.Fatal(err)
	}
	if err := mgr.SetNodeDown(ctx, 5); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.CorruptShard(ctx, res.ID, 0, "flip"); err != nil {
		t.Fatal(err)
	}

	st, err := mgr.Status(ctx, res.ID)
	if err == nil {
		t.Fatalf("超过容错数量时 Status 应返回不可恢复错误")
	}
	var ue *store.UnrecoverableError
	if !errors.As(err, &ue) {
		t.Fatalf("应返回 UnrecoverableError，实际 %T: %v", err, err)
	}
	if ue.ValidShards != 3 || ue.DataShards != 4 {
		t.Fatalf("不可恢复信息应为 3/4 有效分片，实际 %+v", ue)
	}
	if len(ue.UnavailableNo) != 2 || len(ue.CorruptIdx) != 1 {
		t.Fatalf("应分别列出 2 个不可用、1 个损坏，实际 %+v", ue)
	}
	if st == nil || st.Recoverable || st.OKCount != 3 {
		t.Fatalf("检测结果应标记为不可恢复并带明细: %+v", st)
	}

	// Read 同样必须失败，不能返回拼凑出来的错误内容。
	if _, err := mgr.Read(ctx, res.ID, 0, -1); !errors.As(err, &ue) {
		t.Fatalf("超过容错数量时 Read 必须明确返回不可恢复，实际 %v", err)
	}
	// Repair 也不能成功。
	if _, err := mgr.Repair(ctx, res.ID); !errors.As(err, &ue) {
		t.Fatalf("超过容错数量时 Repair 必须明确返回不可恢复，实际 %v", err)
	}

	// 故障数回落到容错范围内（一个节点恢复 => 有效分片 4）后立即可恢复。
	if err := mgr.SetNodeUp(ctx, 4); err != nil {
		t.Fatal(err)
	}
	got, err := mgr.Read(ctx, res.ID, 0, -1)
	if err != nil {
		t.Fatalf("有效分片回到 4 后应能读取: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("恢复后内容错误")
	}
}

// 9. 恰好超过容错数量（全部是内容损坏，且损坏字节本身可读）：
//
//	3 片 flip => 不能把坏片当好片参与解码，必须判不可恢复。
func TestUnrecoverable_ParityPlusOneContentCorruption(t *testing.T) {
	mgr, _, ctx := newTestManager(t, 4, 2)
	data := deterministicData(8000, 19)
	res, err := mgr.Put(ctx, data, 0, 0)
	if err != nil {
		t.Fatalf("Put 失败: %v", err)
	}

	for _, idx := range []int{0, 1, 2} {
		if _, err := mgr.CorruptShard(ctx, res.ID, idx, "flip"); err != nil {
			t.Fatal(err)
		}
	}
	st, err := mgr.Status(ctx, res.ID)
	if err == nil {
		t.Fatalf("3 片内容损坏（>P=2）应不可恢复")
	}
	var ue *store.UnrecoverableError
	if !errors.As(err, &ue) || ue.ValidShards != 3 || len(ue.CorruptIdx) != 3 {
		t.Fatalf("应得到 3 个损坏分片的不可恢复错误，实际 %v, %+v", err, st)
	}
}

// 10. 上传中断 A：暂存完成后崩溃。清单不得发布，随后重新上传必须成功可读。
func TestUploadInterrupt_AfterStaging_NoManifest(t *testing.T) {
	mgr, _, ctx := newTestManager(t, 4, 2)
	data := deterministicData(6000, 23)

	mgr.TestHooks = &store.TestHooks{FailAfterStaging: true}
	_, err := mgr.Put(ctx, data, 0, 0)
	if !errors.Is(err, store.ErrUploadAborted) {
		t.Fatalf("应当模拟上传中断，实际 %v", err)
	}
	mgr.TestHooks = nil

	objs, err := mgr.ListObjects(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 0 {
		t.Fatalf("中断后绝不能发布清单，实际清单: %+v", objs)
	}

	// 重新上传同内容：应当正常发布，且读回正确。
	res, err := mgr.Put(ctx, data, 0, 0)
	if err != nil {
		t.Fatalf("中断后重新上传失败: %v", err)
	}
	got, err := mgr.Read(ctx, res.ID, 0, -1)
	if err != nil {
		t.Fatalf("重新上传的对象无法读取: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("重新上传的对象内容错误")
	}
}

// 11. 上传中断 B：分片已改名到最终位置、但清单事务未提交。
//
//	留下的是无清单孤儿分片：读取端查无对象（404/NotFound），
//	绝不能把它当成一份“完整但读不出来”的清单。
func TestUploadInterrupt_AfterRename_OrphanShardsInvisible(t *testing.T) {
	mgr, _, ctx := newTestManager(t, 4, 2)
	data := deterministicData(6000, 29)
	sum := sha256.Sum256(data)
	id := hex.EncodeToString(sum[:])

	mgr.TestHooks = &store.TestHooks{FailAfterRename: true}
	_, err := mgr.Put(ctx, data, 0, 0)
	if !errors.Is(err, store.ErrUploadAborted) {
		t.Fatalf("应当模拟上传中断，实际 %v", err)
	}
	mgr.TestHooks = nil

	// 清单不存在 => 对外等于对象不存在。
	if _, err := mgr.Read(ctx, id, 0, -1); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("孤儿分片必须对外不可见（NotFound），实际 %v", err)
	}

	// 重新上传：覆盖同名孤儿分片后清单正常发布。
	res, err := mgr.Put(ctx, data, 0, 0)
	if err != nil {
		t.Fatalf("重新上传失败: %v", err)
	}
	got, err := mgr.Read(ctx, res.ID, 0, -1)
	if err != nil {
		t.Fatalf("重新上传后读取失败: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("重新上传后内容错误")
	}
}

// 12. 幂等：同内容重复上传返回同一 ID，不产生重复清单。
func TestPut_Idempotent(t *testing.T) {
	mgr, _, ctx := newTestManager(t, 4, 2)
	data := deterministicData(3333, 31)
	r1, err := mgr.Put(ctx, data, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := mgr.Put(ctx, data, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if r1.ID != r2.ID || !r2.AlreadyExists {
		t.Fatalf("相同内容应幂等返回同一对象，r1=%+v r2=%+v", r1, r2)
	}
}

// 13. 在线节点上的单分片文件丢失（不是整节点 down）：判 unavailable 并可重建。
func TestShardFileMissing_Recoverable(t *testing.T) {
	mgr, _, ctx := newTestManager(t, 4, 2)
	data := deterministicData(7000, 37)
	res, err := mgr.Put(ctx, data, 0, 0)
	if err != nil {
		t.Fatalf("Put 失败: %v", err)
	}

	if err := mgr.DeleteShard(ctx, res.ID, 2); err != nil {
		t.Fatal(err)
	}
	st, err := mgr.Status(ctx, res.ID)
	if err != nil {
		t.Fatalf("Status 失败: %v", err)
	}
	if st.UnavailableNo != 1 || st.CorruptCount != 0 || !st.Recoverable {
		t.Fatalf("在线节点分片丢失应判为 1 个不可用且可恢复，实际 %+v", st)
	}
	got, err := mgr.Read(ctx, res.ID, 0, -1)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("缺 1 片应可重建读取，err=%v", err)
	}
}

// 14. 随机数据的非整块长度，跑一遍完整故障-恢复循环。
func TestRandomData_FullFailureCycle(t *testing.T) {
	mgr, _, ctx := newTestManager(t, 3, 3) // D=3 P=3，容错 3
	data := make([]byte, 9007)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	res, err := mgr.Put(ctx, data, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// 9007 字节 / 3 数据分片 => 每片 3003，共 9009，补齐 2 字节。
	if res.Padding != 2 {
		t.Fatalf("非整块随机数据应记录 padding=2，实际 %d", res.Padding)
	}

	// 第一阶段：容错上限内的 3 类混合故障（1 down、1 内容损坏、1 文件缺失），
	// 读取时重建并“逐块校验”；在线节点上的坏片/缺片会被自动回写修复。
	if err := mgr.SetNodeDown(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.CorruptShard(ctx, res.ID, 1, "flip"); err != nil {
		t.Fatal(err)
	}
	if err := mgr.DeleteShard(ctx, res.ID, 5); err != nil {
		t.Fatal(err)
	}
	got, err := mgr.Read(ctx, res.ID, 100, 8800) // 顺便验证大范围
	if err != nil {
		t.Fatalf("容错上限内应可读取: %v", err)
	}
	if !bytes.Equal(got, data[100:8900]) {
		t.Fatalf("重建后的范围内容错误")
	}

	// 节点恢复后显式修复，全部 6 片回到健康，再开始第二阶段。
	if err := mgr.SetNodeUp(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if st, err := mgr.Repair(ctx, res.ID); err != nil || !st.Healthy {
		t.Fatalf("第一阶段修复后应全健康: st=%+v err=%v", st, err)
	}

	// 第二阶段：恰好超过容错数量（P+1=4 片无效）：2 片内容损坏 + 1 片丢失 + 1 节点 down，
	// 有效分片只剩 2 < D=3，必须明确不可恢复。
	if _, err := mgr.CorruptShard(ctx, res.ID, 1, "flip"); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.CorruptShard(ctx, res.ID, 2, "garbage"); err != nil {
		t.Fatal(err)
	}
	if err := mgr.DeleteShard(ctx, res.ID, 5); err != nil {
		t.Fatal(err)
	}
	if err := mgr.SetNodeDown(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Read(ctx, res.ID, 0, -1); !errors.Is(err, store.ErrUnrecoverable) {
		t.Fatalf("4 片失效（超过容错数 3）后必须不可恢复，实际 %v", err)
	}

	// 节点 0 恢复 => 有效分片恰好回到 3 == D，重新可读，补齐零仍须正确剔除。
	if err := mgr.SetNodeUp(ctx, 0); err != nil {
		t.Fatal(err)
	}
	got, err = mgr.Read(ctx, res.ID, 0, -1)
	if err != nil {
		t.Fatalf("节点恢复后应恰好回到可恢复边界: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("边界恢复后整体内容错误（注意补齐零处理），len=%d", len(got))
	}
}
