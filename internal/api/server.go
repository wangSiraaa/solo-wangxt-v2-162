// Package api 提供无前端的 HTTP/JSON 接口：对象上传、范围读取与模拟故障。
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"ecstore/internal/pgdb"
	"ecstore/internal/service"
)

type Handler struct {
	svc    *service.Service
	totalN int
}

func New(svc *service.Service, totalNodes int) *Handler {
	return &Handler{svc: svc, totalN: totalNodes}
}

func (h *Handler) Mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.healthz)
	mux.HandleFunc("GET /v1/objects", h.listObjects)
	mux.HandleFunc("PUT /v1/objects/{id}", h.upload)
	mux.HandleFunc("GET /v1/objects/{id}", h.getObject)
	mux.HandleFunc("HEAD /v1/objects/{id}", h.headObject)
	mux.HandleFunc("GET /v1/objects/{id}/health", h.objectHealth)
	mux.HandleFunc("POST /v1/objects/{id}/repair", h.repair)
	mux.HandleFunc("POST /v1/objects/{id}/faults/corrupt", h.injectCorrupt)
	mux.HandleFunc("POST /v1/objects/{id}/faults/remove-shard", h.removeShard)
	mux.HandleFunc("POST /v1/nodes/{node}/down", h.nodeDown)
	mux.HandleFunc("POST /v1/nodes/{node}/up", h.nodeUp)
	return mux
}

func (h *Handler) healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func errJSON(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"error": msg})
}

func (h *Handler) listObjects(w http.ResponseWriter, r *http.Request) {
	objs, err := h.svc.List(r.Context())
	if err != nil {
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"objects": objs})
}

// ---- 上传 ----

func (h *Handler) upload(w http.ResponseWriter, r *http.Request) {
	oid := r.PathValue("id")
	if oid == "" {
		oid = r.URL.Query().Get("id")
	}
	if !validID(oid) {
		errJSON(w, http.StatusBadRequest, "对象 id 只能包含字母数字、'-'、'_'、'.'")
		return
	}
	// 客户端中断（如测试“上传中断”）→ r.Body 的读取会失败，
	// Upload 走错误分支清理临时分片，清单不会发布。
	obj, err := h.svc.Upload(r.Context(), oid, r.Body)
	if err != nil {
		switch {
		case errors.Is(err, pgdb.ErrAlreadyExists) || errors.Is(err, service.ErrObjectBusy):
			errJSON(w, http.StatusConflict, err.Error())
		default:
			errJSON(w, http.StatusInternalServerError, "上传失败(已回滚，清单未发布): "+err.Error())
		}
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"id": obj.ID, "size": obj.Size, "stripes": obj.Stripes,
		"data_shards": obj.DataShards, "parity_shards": obj.ParityShards,
		"block_size": obj.BlockSize, "tail_padding": obj.TailPadding,
		"sha256": obj.SHA256,
	})
}

// ---- 读取（支持 Range: bytes=start-end / start- / -suffix）----

func (h *Handler) getObject(w http.ResponseWriter, r *http.Request) {
	oid := r.PathValue("id")

	// 先取大小以便解析 open-ended range
	meta, err := h.svc.Meta(r.Context(), oid)
	if err != nil {
		if errors.Is(err, pgdb.ErrNotFound) {
			errJSON(w, http.StatusNotFound, err.Error())
		} else {
			errJSON(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	start, end, ranged, ok := parseRange(r.Header.Get("Range"), meta.Size)
	if !ok {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", meta.Size))
		errJSON(w, http.StatusRequestedRangeNotSatisfiable, "无法解析 Range")
		return
	}

	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("X-Object-SHA256", meta.SHA256)
	if !ranged {
		// 整对象读取：条带重建 + 逐块校验 + 端到端 sha256
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
		w.WriteHeader(http.StatusOK)
		if meta.Size == 0 {
			return
		}
		n, err := h.svc.ReadAllVerified(r.Context(), oid, w)
		if err != nil {
			h.writeStreamError(w, err, n)
		}
		return
	}

	length := end - start + 1
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Range",
		fmt.Sprintf("bytes %d-%d/%d", start, end, meta.Size))
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	w.WriteHeader(http.StatusPartialContent)
	n, _, err := h.svc.ReadRange(r.Context(), oid, start, length, w)
	if err != nil {
		h.writeStreamError(w, err, n)
	}
}

// writeStreamError 在状态行可能已发送后尽量报告错误。
func (h *Handler) writeStreamError(w http.ResponseWriter, err error, written int64) {
	var ue *service.UnrecoverableError
	if errors.As(err, &ue) {
		log.Printf("[不可恢复] %v (已写出 %d 字节)", ue, written)
		return
	}
	log.Printf("读取流错误(已写出 %d 字节): %v", written, err)
}

func (h *Handler) headObject(w http.ResponseWriter, r *http.Request) {
	oid := r.PathValue("id")
	meta, err := h.svc.Meta(r.Context(), oid)
	if err != nil {
		if errors.Is(err, pgdb.ErrNotFound) {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		return
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
	w.Header().Set("X-Object-SHA256", meta.SHA256)
	w.Header().Set("X-Stripes", strconv.Itoa(meta.Stripes))
	w.Header().Set("X-Tail-Padding", strconv.Itoa(meta.TailPadding))
	w.WriteHeader(http.StatusOK)
}

// parseRange 解析单个字节区间。返回 start, end(含), 是否为范围请求, 是否合法。
// 支持: bytes=START-END、bytes=START-、bytes=-SUFFIX。
func parseRange(hv string, size int64) (int64, int64, bool, bool) {
	if hv == "" {
		return 0, size - 1, false, true
	}
	const prefix = "bytes="
	if !strings.HasPrefix(hv, prefix) {
		return 0, 0, true, false
	}
	spec := strings.TrimSpace(strings.TrimPrefix(hv, prefix))
	if strings.Contains(spec, ",") {
		return 0, 0, true, false // 多区间不支持
	}
	dash := strings.IndexByte(spec, '-')
	if dash < 0 {
		return 0, 0, true, false
	}
	loStr, hiStr := spec[:dash], spec[dash+1:]

	if loStr == "" { // -suffix
		if hiStr == "" {
			return 0, 0, true, false
		}
		suffix, err := strconv.ParseInt(hiStr, 10, 64)
		if err != nil || suffix <= 0 {
			return 0, 0, true, false
		}
		if suffix > size {
			suffix = size
		}
		return size - suffix, size - 1, true, true
	}

	lo, err := strconv.ParseInt(loStr, 10, 64)
	if err != nil || lo < 0 {
		return 0, 0, true, false
	}
	if lo >= size {
		return 0, 0, true, false
	}
	hi := size - 1
	if hiStr != "" {
		hi, err = strconv.ParseInt(hiStr, 10, 64)
		if err != nil || hi < lo {
			return 0, 0, true, false
		}
		if hi >= size {
			hi = size - 1
		}
	}
	return lo, hi, true, true
}

// ---- 健康检查 / 修复 ----

func (h *Handler) objectHealth(w http.ResponseWriter, r *http.Request) {
	rep, err := h.svc.Health(r.Context(), r.PathValue("id"))
	if err != nil {
		if errors.Is(err, pgdb.ErrNotFound) {
			errJSON(w, http.StatusNotFound, err.Error())
		} else {
			errJSON(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	status := http.StatusOK
	if !rep.Recoverable {
		status = http.StatusConflict
	}
	writeJSON(w, status, rep)
}

func (h *Handler) repair(w http.ResponseWriter, r *http.Request) {
	rep, err := h.svc.Repair(r.Context(), r.PathValue("id"))
	if err != nil {
		if errors.Is(err, pgdb.ErrNotFound) {
			errJSON(w, http.StatusNotFound, err.Error())
			return
		}
		if _, ok := err.(*service.UnrecoverableError); ok {
			writeJSON(w, http.StatusConflict, rep)
			return
		}
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

// ---- 模拟故障 ----

type faultReq struct {
	Node   int `json:"node"`
	Stripe int `json:"stripe"`
}

func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		errJSON(w, http.StatusBadRequest, "读取请求体失败: "+err.Error())
		return false
	}
	if len(body) == 0 {
		return true // 允许空 body（字段取零值）
	}
	if err := json.Unmarshal(body, v); err != nil {
		errJSON(w, http.StatusBadRequest, "JSON 解析失败: "+err.Error())
		return false
	}
	return true
}

func (h *Handler) parseNode(w http.ResponseWriter, r *http.Request) (int, bool) {
	n, err := strconv.Atoi(r.PathValue("node"))
	if err != nil || n < 0 || n >= h.totalN {
		errJSON(w, http.StatusBadRequest,
			fmt.Sprintf("节点编号必须在 0..%d 之间", h.totalN-1))
		return 0, false
	}
	return n, true
}

// POST /v1/nodes/{node}/down  {"reason":"..."} —— 节点不可用
func (h *Handler) nodeDown(w http.ResponseWriter, r *http.Request) {
	node, ok := h.parseNode(w, r)
	if !ok {
		return
	}
	var req struct {
		Reason string `json:"reason"`
	}
	decodeBody(w, r, &req)
	if err := h.svc.SetNodeDown(node, req.Reason); err != nil {
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"node": node, "status": "down", "reason": req.Reason,
	})
}

// POST /v1/nodes/{node}/up —— 节点恢复上线（下线期间的分片需要再调 repair）
func (h *Handler) nodeUp(w http.ResponseWriter, r *http.Request) {
	node, ok := h.parseNode(w, r)
	if !ok {
		return
	}
	if err := h.svc.SetNodeUp(node); err != nil {
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"node": node, "status": "up"})
}

// POST /v1/objects/{id}/faults/corrupt {"node":N,"stripe":S}
// 节点保持在线，只翻转一块里一字节 —— 与“节点不可用”明确区分。
func (h *Handler) injectCorrupt(w http.ResponseWriter, r *http.Request) {
	var req faultReq
	if !decodeBody(w, r, &req) {
		return
	}
	if req.Node < 0 || req.Node >= h.totalN {
		errJSON(w, http.StatusBadRequest, fmt.Sprintf("node 必须在 0..%d", h.totalN-1))
		return
	}
	if err := h.svc.CorruptBlock(r.PathValue("id"), req.Node, req.Stripe); err != nil {
		if errors.Is(err, pgdb.ErrNotFound) {
			errJSON(w, http.StatusNotFound, err.Error())
		} else {
			errJSON(w, http.StatusBadRequest, err.Error())
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"object_id": r.PathValue("id"), "node": req.Node,
		"stripe": req.Stripe, "injected": "block-corruption",
	})
}

// POST /v1/objects/{id}/faults/remove-shard {"node":N}
// 节点保持在线，但该对象在它上面的整份分片丢失（第三种读不到的情形）。
func (h *Handler) removeShard(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Node int `json:"node"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	if req.Node < 0 || req.Node >= h.totalN {
		errJSON(w, http.StatusBadRequest, fmt.Sprintf("node 必须在 0..%d", h.totalN-1))
		return
	}
	if err := h.svc.RemoveShard(r.PathValue("id"), req.Node); err != nil {
		errJSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"object_id": r.PathValue("id"), "node": req.Node,
		"injected": "shard-missing",
	})
}

var idAllowed = func() func(r rune) bool {
	allowed := make(map[rune]bool)
	for _, r := range "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_." {
		allowed[r] = true
	}
	return func(r rune) bool { return allowed[r] }
}()

func validID(id string) bool {
	if id == "" || len(id) > 200 || id == "." || id == ".." {
		return false
	}
	for _, r := range id {
		if !idAllowed(r) {
			return false
		}
	}
	return true
}
