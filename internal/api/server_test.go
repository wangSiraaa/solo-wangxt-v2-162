package api_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"ecstore/internal/api"
	"ecstore/internal/erasure"
	"ecstore/internal/nodestore"
	"ecstore/internal/pgdb"
	"ecstore/internal/service"
)

// 测试拓扑：D=4, P=2, 小块(256B)，方便制造“非整块”和多条带对象。
const (
	testD = 4
	testP = 2
	testB = 256
)

type env struct {
	t       *testing.T
	base    string
	svc     *service.Service
	nodes   *nodestore.Store
	client  *http.Client
	cleanup func()
}

// setupEnv 需要真实 PostgreSQL：设置 EC_TEST_DATABASE_URL（指向一个已存在的管理库）。
// 每个测试创建独立数据库，结束后删除；未配置则 t.Skip。
func setupEnv(t *testing.T) *env {
	t.Helper()
	adminURL := os.Getenv("EC_TEST_DATABASE_URL")
	if adminURL == "" {
		t.Skip("跳过集成测试：未设置 EC_TEST_DATABASE_URL")
	}

	ctx := context.Background()
	u, err := url.Parse(adminURL)
	if err != nil {
		t.Fatalf("解析 EC_TEST_DATABASE_URL 失败: %v", err)
	}
	suffix := strings.ReplaceAll(strings.ToLower(fmt.Sprintf("%s_%d",
		strings.ReplaceAll(t.Name(), "/", "_"), time.Now().UnixNano())), " ", "")
	suffix = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			return r
		}
		return '_'
	}, suffix)
	dbName := "ecstore_test_" + suffix
	if len(dbName) > 60 {
		dbName = dbName[:60]
	}

	if err := pgdb.ExecDDLOnURL(ctx, adminURL, "/postgres",
		fmt.Sprintf(`CREATE DATABASE "%s"`, dbName)); err != nil {
		t.Fatalf("创建测试库 %s 失败: %v", dbName, err)
	}

	u.Path = "/" + dbName
	testURL := u.String()
	db, err := pgdb.Connect(ctx, testURL)
	if err != nil {
		t.Fatalf("连接测试库失败: %v", err)
	}

	dir := t.TempDir()
	nodes, err := nodestore.New(filepath.Join(dir, "nodes"), testD+testP)
	if err != nil {
		t.Fatal(err)
	}
	codec, err := erasure.New(testD, testP, testB)
	if err != nil {
		t.Fatal(err)
	}
	svc := service.New(db, nodes, codec)
	ts := httptest.NewServer(api.New(svc, testD+testP).Mux())

	e := &env{
		t: t, base: ts.URL, svc: svc, nodes: nodes,
		client: ts.Client(),
		cleanup: func() {
			ts.Close()
			_ = db.Close(ctx)
			_ = pgdb.ExecDDLOnURL(ctx, adminURL, "/postgres",
				fmt.Sprintf(`DROP DATABASE IF EXISTS "%s" WITH (FORCE)`, dbName))
		},
	}
	t.Cleanup(e.cleanup)
	return e
}

// ---- 小工具 ----

func (e *env) put(oid string, data []byte) (int, map[string]any) {
	e.t.Helper()
	req, _ := http.NewRequest(http.MethodPut, e.base+"/v1/objects/"+oid, bytes.NewReader(data))
	resp, err := e.client.Do(req)
	if err != nil {
		e.t.Fatalf("PUT: %v", err)
	}
	defer resp.Body.Close()
	return resp.StatusCode, decodeJSON(e.t, resp.Body)
}

func (e *env) get(oid, rangeHdr string) (int, http.Header, []byte) {
	e.t.Helper()
	req, _ := http.NewRequest(http.MethodGet, e.base+"/v1/objects/"+oid, nil)
	if rangeHdr != "" {
		req.Header.Set("Range", rangeHdr)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		e.t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, b
}

func (e *env) post(path string, body any) (int, map[string]any) {
	e.t.Helper()
	var rdr io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		rdr = bytes.NewReader(raw)
	}
	req, _ := http.NewRequest(http.MethodPost, e.base+path, rdr)
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.client.Do(req)
	if err != nil {
		e.t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	return resp.StatusCode, decodeJSON(e.t, resp.Body)
}

func decodeJSON(t *testing.T, r io.Reader) map[string]any {
	t.Helper()
	b, _ := io.ReadAll(r)
	if len(b) == 0 {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("非 JSON 响应: %s", b)
	}
	return m
}

func (e *env) health(oid string) map[string]any {
	e.t.Helper()
	req, _ := http.NewRequest(http.MethodGet, e.base+"/v1/objects/"+oid+"/health", nil)
	resp, err := e.client.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 && resp.StatusCode != 409 {
		b, _ := io.ReadAll(resp.Body)
		e.t.Fatalf("health status=%d body=%s", resp.StatusCode, b)
	}
	return decodeJSON(e.t, resp.Body)
}

func (e *env) nodeDown(n int) {
	e.t.Helper()
	if st, m := e.post(fmt.Sprintf("/v1/nodes/%d/down", n), map[string]string{"reason": "test"}); st != 200 {
		e.t.Fatalf("node %d down: %d %v", n, st, m)
	}
}
func (e *env) nodeUp(n int) {
	e.t.Helper()
	if st, m := e.post(fmt.Sprintf("/v1/nodes/%d/up", n), nil); st != 200 {
		e.t.Fatalf("node %d up: %d %v", n, st, m)
	}
}
func (e *env) corrupt(oid string, n, stripe int) {
	e.t.Helper()
	if st, m := e.post("/v1/objects/"+oid+"/faults/corrupt",
		map[string]int{"node": n, "stripe": stripe}); st != 200 {
		e.t.Fatalf("corrupt: %d %v", st, m)
	}
}
func (e *env) removeShard(oid string, n int) {
	e.t.Helper()
	if st, m := e.post("/v1/objects/"+oid+"/faults/remove-shard",
		map[string]int{"node": n}); st != 200 {
		e.t.Fatalf("remove-shard: %d %v", st, m)
	}
}

func sha(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func randomData(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

// ---- 测试用例 ----

// 1) 空对象
func TestEmptyObject(t *testing.T) {
	e := setupEnv(t)
	st, m := e.put("empty", nil)
	if st != 201 {
		t.Fatalf("空对象上传 status=%d body=%v", st, m)
	}
	if m["size"].(float64) != 0 || m["stripes"].(float64) != 0 ||
		m["tail_padding"].(float64) != 0 {
		t.Fatalf("空对象清单元数据错误: %v", m)
	}
	if m["sha256"].(string) != sha(nil) {
		t.Fatalf("空对象 sha256 应为 sha256(\"\")")
	}
	rg, hdr, body := e.get("empty", "")
	if rg != 200 || len(body) != 0 {
		t.Fatalf("空对象读取: status=%d len=%d", rg, len(body))
	}
	if hdr.Get("Content-Length") != "0" {
		t.Fatalf("空对象 Content-Length=%s", hdr.Get("Content-Length"))
	}
	// 范围读空对象
	if rg, _, body := e.get("empty", "bytes=0-0"); rg != 416 {
		t.Fatalf("空对象越界范围应 416, got %d body=%q", rg, body)
	}
	// 健康检查：可恢复
	h := e.health("empty")
	if h["recoverable"] != true || h["healthy"] != true {
		t.Fatalf("空对象健康: %v", h)
	}
}

// 2) 非整块长度：1 字节、块边界、块边界±1、多条带
func TestNonAlignedLengthsAndRanges(t *testing.T) {
	e := setupEnv(t)
	sizes := []int{1, testB - 1, testB, testB + 1,
		testD*testB - 1, testD * testB, testD*testB + 1,
		3*testD*testB + 37}
	for idx, n := range sizes {
		oid := fmt.Sprintf("obj-%d", idx)
		data := randomData(t, n)
		st, m := e.put(oid, data)
		if st != 201 {
			t.Fatalf("size=%d 上传失败: %v", n, m)
		}
		wantPad := (testD*testB - n%(testD*testB)) % (testD * testB)
		if int(m["tail_padding"].(float64)) != wantPad {
			t.Fatalf("size=%d tail_padding=%v want %d", n, m["tail_padding"], wantPad)
		}

		// 全量读必须字节一致 —— 验证补零没有被当作原文件内容返回
		rg, _, body := e.get(oid, "")
		if rg != 200 || !bytes.Equal(body, data) {
			t.Fatalf("size=%d 全量读不一致 status=%d len=%d", n, rg, len(body))
		}
		// 末尾若干字节必须是真实数据而非零
		if n > 0 && body[len(body)-1] != data[n-1] {
			t.Fatalf("size=%d 末字节错误（疑似补零泄漏）", n)
		}

		// 覆盖各类 range
		ranges := [][2]int{{0, 0}, {n - 1, n - 1}, {0, n - 1}}
		if mid, hi := n/2, n/2+9; hi < n {
			ranges = append(ranges, [2]int{mid, hi})
		}
		for _, rng := range ranges {
			spec := fmt.Sprintf("bytes=%d-%d", rng[0], rng[1])
			rg, hdr, got := e.get(oid, spec)
			if rg != 206 {
				t.Fatalf("size=%d range=%s status=%d", n, spec, rg)
			}
			want := data[rng[0] : rng[1]+1]
			if !bytes.Equal(got, want) {
				t.Fatalf("size=%d range=%s 内容不一致", n, spec)
			}
			if !strings.HasPrefix(hdr.Get("Content-Range"),
				fmt.Sprintf("bytes %d-%d/%d", rng[0], rng[1], n)) {
				t.Fatalf("Content-Range 错误: %s", hdr.Get("Content-Range"))
			}
		}
		// open-ended 与 suffix 形式（suffix 数量超过长度时服务端应裁剪到开头）
		if _, _, got := e.get(oid, fmt.Sprintf("bytes=%d-", n/3)); !bytes.Equal(got, data[n/3:]) {
			t.Fatalf("size=%d open-ended range 不一致", n)
		}
		suffix := 5
		if suffix > n {
			suffix = n
		}
		if _, _, got := e.get(oid, "bytes=-5"); !bytes.Equal(got, data[n-suffix:]) {
			t.Fatalf("size=%d suffix range 不一致", n)
		}
		// 完全越界的 range 应得 416
		if rg, _, _ := e.get(oid, fmt.Sprintf("bytes=%d-", n)); rg != 416 {
			t.Fatalf("size=%d start==size 应 416, got %d", n, rg)
		}
	}
}

// 3) 容错边界：恰好 P 个失效可读；P+1 个失效明确不可恢复。
func TestFaultToleranceBoundary(t *testing.T) {
	e := setupEnv(t)
	// 多条带对象：2.5 个条带
	data := randomData(t, 2*testD*testB+testB+7)
	st, m := e.put("boundary", data)
	if st != 201 {
		t.Fatalf("上传: %v", m)
	}
	stripes := int(m["stripes"].(float64))

	// ---- 恰好 P=2：一个节点下线 + 一个节点内容损坏（两种故障分开检测）----
	e.nodeDown(4)
	e.corrupt("boundary", 0, 0)
	if rg, _, body := e.get("boundary", ""); rg != 200 || !bytes.Equal(body, data) {
		t.Fatalf("恰好 %d 失效时应可读且一致: status=%d", testP, rg)
	}
	h := e.health("boundary")
	if h["recoverable"] != true {
		t.Fatalf("恰好 P 失效应可恢复: %v", h)
	}
	if h["healthy"] != false {
		t.Fatalf("存在故障时 healthy 应为 false")
	}
	shards := h["shards"].([]any)
	st0 := shards[0].(map[string]any)
	st4 := shards[4].(map[string]any)
	if st0["corrupt_blocks"].(float64) != 1 {
		t.Fatalf("node0 应报告 1 个 corrupt 块: %v", st0)
	}
	if st4["unavailable_blocks"].(float64) != float64(stripes) || st4["node_up"] != false {
		t.Fatalf("node4 应报告全部块 unavailable: %v", st4)
	}

	// ---- 超过容错数：再下线节点 1 => 好块 D-1=3 < D=4 ----
	e.nodeDown(1)
	rg, _, body := e.get("boundary", "")
	if rg != 200 || len(body) != 0 {
		t.Fatalf("不可恢复时不应写出数据: status=%d len=%d", rg, len(body))
	}
	h = e.health("boundary")
	if h["recoverable"] != false {
		t.Fatalf("P+1 失效必须报告不可恢复")
	}
	if h["min_good_blocks"].(float64) != float64(testD-1) {
		t.Fatalf("min_good_blocks=%v want %d", h["min_good_blocks"], testD-1)
	}
	// repair 也必须明确拒绝，不能静默产生坏数据
	if code, rm := e.post("/v1/objects/boundary/repair", nil); code != 409 {
		t.Fatalf("不可恢复时 repair 应 409, got %d %v", code, rm)
	} else if rm["recoverable"] != false {
		t.Fatalf("repair 响应应标记 recoverable=false: %v", rm)
	}

	// 范围读取落在受损条带也拿不到数据（206 状态行已先发出，但 body 必须为空，
	// 绝不能吐出部分/错误字节）
	if rg, _, rb := e.get("boundary", "bytes=0-10"); rg != 206 || len(rb) != 0 {
		t.Fatalf("不可恢复对象的范围读应中止且无数据: status=%d len=%d", rg, len(rb))
	}
}

// 4) 损坏数据分片 vs 损坏校验分片，重建后修复并逐块校验
func TestRepairCorruptAndMissing(t *testing.T) {
	e := setupEnv(t)
	data := randomData(t, testD*testB+testD*testB/2)
	if st, m := e.put("repair", data); st != 201 {
		t.Fatalf("上传: %v", m)
	}

	// 数据分片 0 损坏；校验分片 5 整份丢失（节点在线）；节点 3 下线
	e.corrupt("repair", 0, 0)
	e.corrupt("repair", 0, 1)
	e.removeShard("repair", 5)
	e.nodeDown(3)

	// 好块 = 6 - 3 = 3 ... 不够！先只注入 2 个故障
	e.nodeUp(3)

	h := e.health("repair")
	if h["recoverable"] != true {
		t.Fatalf("两故障应可恢复: %v", h)
	}
	// 读得出且正确（读路径不修改节点）
	if _, _, body := e.get("repair", ""); !bytes.Equal(body, data) {
		t.Fatal("两故障下内容不一致")
	}

	// repair：重建并写回在线的坏/缺节点（node0、node5）
	code, rm := e.post("/v1/objects/repair/repair", nil)
	if code != 200 {
		t.Fatalf("repair: %d %v", code, rm)
	}
	rebuiltAll := map[int]bool{}
	for _, s := range rm["stripes"].([]any) {
		for _, n := range s.(map[string]any)["rebuilt_nodes"].([]any) {
			rebuiltAll[int(n.(float64))] = true
		}
	}
	if !rebuiltAll[0] || !rebuiltAll[5] {
		t.Fatalf("node0、node5 都应被重建, got %v", rebuiltAll)
	}
	h = e.health("repair")
	if h["healthy"] != true {
		t.Fatalf("修复后应完全健康: %v", h)
	}
	if _, _, body := e.get("repair", ""); !bytes.Equal(body, data) {
		t.Fatal("修复后内容不一致")
	}
}

// 5) 下线节点恢复上线后 repair 能把下线期间错过的修复补齐
func TestNodeComesBackThenRepair(t *testing.T) {
	e := setupEnv(t)
	data := randomData(t, testD*testB+10)
	if st, m := e.put("comeback", data); st != 201 {
		t.Fatal(m)
	}
	// 下线 4、5，损坏 0 —— 好块=3<4：此时不可修复
	e.nodeDown(4)
	e.nodeDown(5)
	e.corrupt("comeback", 0, 0)
	if code, _ := e.post("/v1/objects/comeback/repair", nil); code != 409 {
		t.Fatalf("好块不足应 409, got %d", code)
	}
	// 节点恢复（分片文件仍然完好），repair 成功
	e.nodeUp(4)
	e.nodeUp(5)
	if code, rm := e.post("/v1/objects/comeback/repair", nil); code != 200 {
		t.Fatalf("节点回来后 repair 应成功: %d %v", code, rm)
	}
	h := e.health("comeback")
	if h["healthy"] != true {
		t.Fatalf("应完全健康: %v", h)
	}
	if _, _, body := e.get("comeback", ""); !bytes.Equal(body, data) {
		t.Fatal("内容不一致")
	}
}

// 6) 上传中断：客户端中途断连，绝不发布完整清单，也不留临时垃圾
func TestUploadInterruptedNeverPublishes(t *testing.T) {
	e := setupEnv(t)
	pr, pw := io.Pipe()
	go func() {
		_, _ = pw.Write(make([]byte, testB))       // 先写一条的零头
		_ = pw.CloseWithError(io.ErrUnexpectedEOF) // 模拟客户端中断
	}()
	req, _ := http.NewRequest(http.MethodPut, e.base+"/v1/objects/interrupted", pr)
	resp, err := e.client.Do(req)
	if err == nil {
		defer resp.Body.Close()
		if resp.StatusCode == 201 {
			t.Fatal("中断上传绝不能返回 201")
		}
		_, _ = io.Copy(io.Discard, resp.Body)
	}

	// 清单必须不存在
	req, _ = http.NewRequest(http.MethodHead, e.base+"/v1/objects/interrupted", nil)
	r2, err := e.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	r2.Body.Close()
	if r2.StatusCode != 404 {
		t.Fatalf("中断后 HEAD 应为 404, got %d", r2.StatusCode)
	}

	// 同名对象应能重新成功上传（临时分片已清理，不会残留）
	data := randomData(t, testD*testB+5)
	if st, m := e.put("interrupted", data); st != 201 {
		t.Fatalf("中断后重传应成功: %d %v", st, m)
	}
	if _, _, body := e.get("interrupted", ""); !bytes.Equal(body, data) {
		t.Fatal("重传内容不一致")
	}
}

// 7) 清单不允许指向缺失分片：删一份分片文件 + 对应 DB 行仍在时，读取显式失败而非吐零
func TestMissingShardReportedNotAsData(t *testing.T) {
	e := setupEnv(t)
	data := randomData(t, testD*testB+11)
	if st, m := e.put("missing", data); st != 201 {
		t.Fatal(m)
	}
	// 在线节点删分片：缺失必须被检测为 missing（不是读成全零）
	e.removeShard("missing", 2)
	h := e.health("missing")
	shards := h["shards"].([]any)
	st2 := shards[2].(map[string]any)
	if st2["missing_blocks"].(float64) < 1 || st2["node_up"] != true {
		t.Fatalf("在线节点缺分片应报 missing: %v", st2)
	}
	// 单缺失仍可读
	if _, _, body := e.get("missing", ""); !bytes.Equal(body, data) {
		t.Fatal("单分片缺失时内容应可重建一致")
	}
}

// 8) 重复上传冲突
func TestDuplicateUploadConflict(t *testing.T) {
	e := setupEnv(t)
	data := randomData(t, 10)
	if st, _ := e.put("dup", data); st != 201 {
		t.Fatal("首次上传应成功")
	}
	if st, _ := e.put("dup", data); st != 409 {
		t.Fatalf("重复上传应 409, got %d", st)
	}
}

// 9) 404
// 10) 并发同名上传：只能有一个成功，成功的对象必须可读
func TestConcurrentSameNameUploads(t *testing.T) {
	e := setupEnv(t)
	const N = 4
	var wg sync.WaitGroup
	statuses := make([]int, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			data := randomData(t, testD*testB+i+1) // 每个 goroutine 数据不同
			req, _ := http.NewRequest(http.MethodPut, e.base+"/v1/objects/race", bytes.NewReader(data))
			resp, err := e.client.Do(req)
			if err != nil {
				t.Errorf("并发上传 %d 请求失败: %v", i, err)
				return
			}
			defer resp.Body.Close()
			statuses[i] = resp.StatusCode
			_, _ = io.Copy(io.Discard, resp.Body)
		}(i)
	}
	wg.Wait()
	created, conflicts := 0, 0
	for _, st := range statuses {
		switch st {
		case 201:
			created++
		case 409:
			conflicts++
		default:
			t.Fatalf("并发上传出现非预期状态码 %d", st)
		}
	}
	if created != 1 || conflicts != N-1 {
		t.Fatalf("并发同名上传应有 1 个成功 %d 个冲突, got created=%d conflicts=%d", N-1, created, conflicts)
	}
	// 唯一发布成功的清单指向的分片必须完整可读
	if _, _, body := e.get("race", ""); len(body) == 0 {
		t.Fatal("并发竞争后对象不应为空（每个候选都非空）")
	}
}

func TestNotFound(t *testing.T) {
	e := setupEnv(t)
	if rg, _, _ := e.get("nope", ""); rg != 404 {
		t.Fatalf("不存在对象应 404, got %d", rg)
	}
}
