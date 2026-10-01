package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"erasure-store/internal/api"
	"erasure-store/internal/store"
)

func newTestServer(t *testing.T) (*httptest.Server, *store.Manager, context.Context) {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("创建数据库连接失败: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM shards`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM objects`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE nodes SET state='up'`); err != nil {
		t.Fatal(err)
	}

	mgr, err := store.NewManager(pool, t.TempDir(), 4, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.EnsureNodes(ctx, 6); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(api.NewServer(mgr, 64<<20).Routes())
	t.Cleanup(srv.Close)
	t.Cleanup(pool.Close)
	return srv, mgr, ctx
}

type putResp struct {
	ID            string `json:"id"`
	Size          int64  `json:"size"`
	Padding       int64  `json:"padding"`
	AlreadyExists bool   `json:"already_exists"`
}

func doPut(t *testing.T, srv *httptest.Server, data []byte) putResp {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/objects", bytes.NewReader(data))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("上传应返回 201，实际 %d: %s", resp.StatusCode, body)
	}
	var pr putResp
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		t.Fatal(err)
	}
	return pr
}

func postJSON(t *testing.T, url string, body any) (int, map[string]any) {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(http.MethodPost, url, r)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	return resp.StatusCode, m
}

// HTTP 全链路：上传（含空对象）、整体读、范围读、检测、故障注入、不可恢复、修复。
func TestHTTP_EndToEnd(t *testing.T) {
	srv, _, _ := newTestServer(t)

	// 1. 空对象
	pr := doPut(t, srv, []byte{})
	if pr.Size != 0 || pr.Padding != 0 {
		t.Fatalf("空对象元数据错误: %+v", pr)
	}
	resp, err := http.Get(srv.URL + "/objects/" + pr.ID)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || len(body) != 0 {
		t.Fatalf("空对象读取应 200 且无内容，实际 %d body=%x", resp.StatusCode, body)
	}

	// 2. 非整块对象（4003 字节，padding=1）
	data := make([]byte, 4003)
	for i := range data {
		data[i] = byte(i)
	}
	data[4002] = 0 // 真实尾零，必须保留
	pr = doPut(t, srv, data)
	if pr.Padding != 1 {
		t.Fatalf("padding 应为 1，实际 %d", pr.Padding)
	}

	// 整体读
	resp, err = http.Get(srv.URL + "/objects/" + pr.ID)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Equal(got, data) {
		t.Fatalf("整体读回内容/长度错误：%d vs %d", len(got), len(data))
	}

	// 3. Range 头范围读取 bytes=4000-4002 => 最后 3 个真实字节
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/objects/"+pr.ID, nil)
	req.Header.Set("Range", "bytes=4000-4002")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	got, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(got, data[4000:4003]) {
		t.Fatalf("Range 读取错误：status=%d body=%x", resp.StatusCode, got)
	}

	// 查询参数范围读取
	resp, err = http.Get(srv.URL + "/objects/" + pr.ID + "?start=0&end=4")
	if err != nil {
		t.Fatal(err)
	}
	got, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 206 || !bytes.Equal(got, data[0:4]) {
		t.Fatalf("查询参数范围读取错误：status=%d body=%x", resp.StatusCode, got)
	}

	// 4. 节点不可用 ×2（==容错数）：仍 200
	if code, _ := postJSON(t, srv.URL+"/nodes/4/down", nil); code != 200 {
		t.Fatalf("节点下线应 200，实际 %d", code)
	}
	if code, _ := postJSON(t, srv.URL+"/nodes/5/down", nil); code != 200 {
		t.Fatalf("节点下线应 200，实际 %d", code)
	}
	resp, err = http.Get(srv.URL + "/objects/" + pr.ID)
	if err != nil {
		t.Fatal(err)
	}
	got, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !bytes.Equal(got, data) {
		t.Fatalf("2 节点 down（容错上限）应仍可读，status=%d len=%d", resp.StatusCode, len(got))
	}

	// status 显示可恢复
	resp, err = http.Get(srv.URL + "/objects/" + pr.ID + "/status")
	if err != nil {
		t.Fatal(err)
	}
	var st map[string]any
	json.NewDecoder(resp.Body).Decode(&st)
	resp.Body.Close()
	if st["recoverable"] != true || st["healthy"] != false {
		t.Fatalf("status 应 recoverable=true healthy=false，实际 %v", st)
	}

	// 5. 再损坏一个分片（第 0 片）=> 超过容错数 => 不可恢复
	if code, m := postJSON(t, srv.URL+"/objects/"+pr.ID+"/shards/0/corrupt", map[string]string{"mode": "flip"}); code != 200 {
		t.Fatalf("注入损坏应 200，实际 %d: %v", code, m)
	}
	resp, err = http.Get(srv.URL + "/objects/" + pr.ID)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("超过容错数量应 409 明确不可恢复，实际 %d: %s", resp.StatusCode, body)
	}
	if !bytes.Contains(body, []byte("不可恢复")) {
		t.Fatalf("错误信息必须明确写出“不可恢复”，实际 %s", body)
	}

	// status 同样 409 且带逐片明细
	resp, err = http.Get(srv.URL + "/objects/" + pr.ID + "/status")
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 409 || !bytes.Contains(body, []byte(`"unavailable_count":2`)) ||
		!bytes.Contains(body, []byte(`"corrupt_count":1`)) {
		t.Fatalf("不可恢复的 status 应带分类计数，实际 %d: %s", resp.StatusCode, body)
	}

	// 6. 恢复一个节点 => 回到容错边界 => repair 200 且逐块校验通过
	if code, _ := postJSON(t, srv.URL+"/nodes/4/up", nil); code != 200 {
		t.Fatalf("节点恢复应 200")
	}
	if code, m := postJSON(t, srv.URL+"/objects/"+pr.ID+"/repair", nil); code != 200 {
		t.Fatalf("边界条件下修复应 200，实际 %d: %v", code, m)
	}
	resp, err = http.Get(srv.URL + "/objects/" + pr.ID)
	if err != nil {
		t.Fatal(err)
	}
	got, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Equal(got, data) {
		t.Fatalf("修复后内容应完整正确（含补齐零剔除），len=%d", len(got))
	}

	// 7. 不存在的对象 404
	resp, _ = http.Get(srv.URL + "/objects/deadbeef")
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("不存在对象应 404，实际 %d", resp.StatusCode)
	}
}

// 上传中断钩子（在 store 层注入 TestHooks）经 HTTP 也不能出现可读的完整清单。
func TestHTTP_UploadAbortLeavesNoManifest(t *testing.T) {
	srv, mgr, ctx := newTestServer(t)

	data := make([]byte, 5000)
	mgr.TestHooks = &store.TestHooks{FailAfterRename: true}
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/objects", bytes.NewReader(data))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("模拟中断应返回 5xx，实际 %d: %s", resp.StatusCode, body)
	}
	mgr.TestHooks = nil

	// 对象列表必须为空：没有发布任何清单。
	resp, err = http.Get(srv.URL + "/objects")
	if err != nil {
		t.Fatal(err)
	}
	var objs []map[string]any
	json.NewDecoder(resp.Body).Decode(&objs)
	resp.Body.Close()
	if len(objs) != 0 {
		t.Fatalf("上传中断后清单必须为空，实际 %v", objs)
	}
	_ = ctx
}
