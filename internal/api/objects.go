package api

import (
	"errors"
	"io"
	"net/http"
	"strconv"

	"erasure-store/internal/store"
)

// putObject: PUT /objects
// 请求体即对象原始字节；可用查询参数 data_shards / parity_shards 覆盖默认编码布局。
func (s *Server) putObject(w http.ResponseWriter, r *http.Request) {
	dataShards := atoiDefault(r.URL.Query().Get("data_shards"), 0)
	parityShards := atoiDefault(r.URL.Query().Get("parity_shards"), 0)

	// 教学项目对象体积受限，多读 1 字节以判断是否超限。
	data, err := io.ReadAll(io.LimitReader(r.Body, s.MaxObject+1))
	if err != nil {
		writeError(w, http.StatusBadRequest, "读取请求体失败: %v", err)
		return
	}
	if int64(len(data)) > s.MaxObject {
		writeError(w, http.StatusRequestEntityTooLarge,
			"对象超过大小上限 %d 字节（可用 MAX_OBJECT_BYTES 调整）", s.MaxObject)
		return
	}

	res, err := s.Mgr.Put(r.Context(), data, dataShards, parityShards)
	if err != nil {
		mapStoreError(w, err)
		return
	}
	status := http.StatusCreated
	if res.AlreadyExists {
		status = http.StatusOK
	}
	w.Header().Set("Location", "/objects/"+res.ID)
	writeJSON(w, status, res)
}

// listObjects: GET /objects
func (s *Server) listObjects(w http.ResponseWriter, r *http.Request) {
	objs, err := s.Mgr.ListObjects(r.Context())
	if err != nil {
		mapStoreError(w, err)
		return
	}
	if objs == nil {
		objs = []store.ObjectMeta{}
	}
	writeJSON(w, http.StatusOK, objs)
}

// getObject: GET /objects/{id}
// 范围读取两种方式：
//   - HTTP 标准 Range: Range: bytes=10-20
//   - 查询参数: ?start=10&end=20（end 不含；end 省略读到末尾）
func (s *Server) getObject(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	var start, end int64 = 0, -1
	ranged := false

	if sp := r.URL.Query().Get("start"); sp != "" {
		v, err := strconv.ParseInt(sp, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "start 不是整数: %v", err)
			return
		}
		start = v
		ranged = true
	}
	if ep := r.URL.Query().Get("end"); ep != "" {
		v, err := strconv.ParseInt(ep, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "end 不是整数: %v", err)
			return
		}
		end = v
		ranged = true
	}
	if rh := r.Header.Get("Range"); rh != "" {
		rs, re, ok, err := parseSingleRange(rh)
		if err != nil || !ok {
			writeError(w, http.StatusRequestedRangeNotSatisfiable,
				"不支持的 Range 头（仅支持单个 bytes=start-end 或 bytes=start- 区间；不支持后缀范围）: %q", rh)
			return
		}
		start, end = rs, re
		ranged = true
	}

	var length int64 = -1
	if ranged && end >= 0 {
		if end <= start {
			writeError(w, http.StatusBadRequest, "范围非法：end(%d) 必须大于 start(%d)", end, start)
			return
		}
		length = end - start
	}

	data, err := s.Mgr.Read(r.Context(), id, start, length)
	if err != nil {
		mapStoreError(w, err)
		return
	}
	if ranged {
		w.Header().Set("Content-Range", contentRange(start, data))
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
	}
	_, _ = w.Write(data)
}

// status: GET /objects/{id}/status
func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	st, err := s.Mgr.Status(r.Context(), id)
	if err != nil {
		var ue *store.UnrecoverableError
		if errors.As(err, &ue) {
			// 仍然把检测明细带回去，用 409 表达“已达不可恢复状态”。
			writeJSON(w, http.StatusConflict, map[string]any{
				"status": st,
				"error":  err.Error(),
			})
			return
		}
		mapStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

// repair: POST /objects/{id}/repair
func (s *Server) repair(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	st, err := s.Mgr.Repair(r.Context(), id)
	if err != nil {
		mapStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

// deleteObject: DELETE /objects/{id}
func (s *Server) deleteObject(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.Mgr.DeleteObject(r.Context(), id); err != nil {
		mapStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"deleted": id})
}

// parseSingleRange 解析 "bytes=start-end"。
// 返回 end 为“不含的上界”，end=-1 表示到对象末尾。
// 仅支持 "bytes=start-end" 与 "bytes=start-"；不支持后缀范围 "bytes=-N"（需要先知道对象大小）。
func parseSingleRange(h string) (start int64, end int64, ok bool, err error) {
	const prefix = "bytes="
	if len(h) < len(prefix) || h[:len(prefix)] != prefix {
		return 0, 0, false, nil
	}
	spec := h[len(prefix):]
	// 只接受单个区间
	if spec == "" || spec[0] == '-' {
		// 空区间或后缀范围
		return 0, 0, false, nil
	}
	dash := -1
	for i, c := range spec {
		if c == ',' {
			return 0, 0, false, nil
		}
		if c == '-' {
			if dash >= 0 {
				return 0, 0, false, nil
			}
			dash = i
		}
	}
	if dash < 0 {
		return 0, 0, false, nil
	}
	lo, hi := spec[:dash], spec[dash+1:]
	start, err = strconv.ParseInt(lo, 10, 64)
	if err != nil {
		return 0, 0, false, err
	}
	if start < 0 {
		return 0, 0, false, nil
	}
	if hi == "" {
		return start, -1, true, nil
	}
	last, err := strconv.ParseInt(hi, 10, 64)
	if err != nil {
		return 0, 0, false, err
	}
	if last < start {
		return 0, 0, false, nil
	}
	return start, last + 1, true, nil
}

func contentRange(start int64, data []byte) string {
	// data 已按对象实际尾部截断，total 用 start+len 作为区间末端的近似展示；
	// 教学接口主要靠 ?start/end，这里给出合法的 Content-Range 头格式。
	return "bytes " + strconv.FormatInt(start, 10) + "-" +
		strconv.FormatInt(start+int64(len(data))-1, 10) + "/*"
}

func atoiDefault(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}
