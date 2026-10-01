// Package api 提供对象上传、范围读取与模拟故障的 HTTP 接口。
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"

	"erasure-store/internal/store"
)

// Server 持有 HTTP 路由依赖。
type Server struct {
	Mgr       *store.Manager
	MaxObject int64 // 单次上传对象大小上限（字节）
}

func NewServer(mgr *store.Manager, maxObjectBytes int64) *Server {
	return &Server{Mgr: mgr, MaxObject: maxObjectBytes}
}

// Routes 在 Go 1.22 的 ServeMux 上注册全部路由。
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", s.health)

	mux.HandleFunc("PUT /objects", s.putObject)            // 上传（请求体即对象内容）
	mux.HandleFunc("GET /objects", s.listObjects)          // 对象清单
	mux.HandleFunc("GET /objects/{id}", s.getObject)       // 整对象读取；Range 头做范围读取
	mux.HandleFunc("GET /objects/{id}/status", s.status)   // 逐块检测（不修复）
	mux.HandleFunc("POST /objects/{id}/repair", s.repair)  // 达到条件时重建
	mux.HandleFunc("DELETE /objects/{id}", s.deleteObject) // 测试清理

	mux.HandleFunc("GET /nodes", s.listNodes)
	mux.HandleFunc("POST /nodes/{node}/down", s.nodeDown) // 模拟节点不可用
	mux.HandleFunc("POST /nodes/{node}/up", s.nodeUp)     // 模拟节点恢复

	// 模拟分片故障
	mux.HandleFunc("POST /objects/{id}/shards/{shard}/corrupt", s.corruptShard) // body: {"mode":"flip|truncate|garbage"}
	mux.HandleFunc("DELETE /objects/{id}/shards/{shard}", s.deleteShard)        // 模拟在线节点上分片丢失

	return mux
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// writeJSON 统一 JSON 输出。
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("写 JSON 响应失败: %v", err)
	}
}

type errResp struct {
	Error string `json:"error"`
}

func writeError(w http.ResponseWriter, status int, format string, args ...any) {
	writeJSON(w, status, errResp{Error: fmt.Sprintf(format, args...)})
}

// mapStoreError 把 store 层错误映射为合适的 HTTP 状态码。
func mapStoreError(w http.ResponseWriter, err error) {
	var nf *store.NotFoundError
	var ue *store.UnrecoverableError
	switch {
	case errors.As(err, &nf):
		writeError(w, http.StatusNotFound, "%s", err.Error())
	case errors.As(err, &ue):
		// 409 Conflict + 明确的不可恢复信息
		writeError(w, http.StatusConflict, "不可恢复: %s", err.Error())
	case errors.Is(err, store.ErrUploadAborted):
		writeError(w, http.StatusInternalServerError, "%s（模拟的上传中断，清单未发布）", err.Error())
	default:
		writeError(w, http.StatusBadRequest, "%s", err.Error())
	}
}
