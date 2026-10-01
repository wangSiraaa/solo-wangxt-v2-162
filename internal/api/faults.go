package api

import (
	"encoding/json"
	"net/http"
	"strconv"
)

// listNodes: GET /nodes
func (s *Server) listNodes(w http.ResponseWriter, r *http.Request) {
	nodes, err := s.Mgr.ListNodes(r.Context())
	if err != nil {
		mapStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nodes)
}

// nodeDown: POST /nodes/{node}/down —— 模拟整节点不可用（目录文件不动）。
func (s *Server) nodeDown(w http.ResponseWriter, r *http.Request) {
	node, err := strconv.Atoi(r.PathValue("node"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "节点编号必须是整数")
		return
	}
	if err := s.Mgr.SetNodeDown(r.Context(), node); err != nil {
		mapStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"node": node, "state": "down"})
}

// nodeUp: POST /nodes/{node}/up —— 模拟节点恢复。
func (s *Server) nodeUp(w http.ResponseWriter, r *http.Request) {
	node, err := strconv.Atoi(r.PathValue("node"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "节点编号必须是整数")
		return
	}
	if err := s.Mgr.SetNodeUp(r.Context(), node); err != nil {
		mapStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"node": node, "state": "up"})
}

type corruptReq struct {
	Mode string `json:"mode"` // flip（翻转首字节）/ truncate（截断）/ garbage（置零）
}

// corruptShard: POST /objects/{id}/shards/{shard}/corrupt
// 模拟分片内容损坏：节点仍在线、文件仍在，但校验值对不上。
func (s *Server) corruptShard(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	shard, err := strconv.Atoi(r.PathValue("shard"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "分片编号必须是整数")
		return
	}
	req := corruptReq{Mode: "flip"}
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "解析请求体失败: %v", err)
			return
		}
	}
	actual, err := s.Mgr.CorruptShard(r.Context(), id, shard, req.Mode)
	if err != nil {
		mapStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"object": id, "shard": shard, "mode": req.Mode, "actual_checksum": actual,
	})
}

// deleteShard: DELETE /objects/{id}/shards/{shard}
// 模拟在线节点上的分片文件丢失（区别于节点整机 down）。
func (s *Server) deleteShard(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	shard, err := strconv.Atoi(r.PathValue("shard"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "分片编号必须是整数")
		return
	}
	if err := s.Mgr.DeleteShard(r.Context(), id, shard); err != nil {
		mapStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted_shard": shard, "object": id})
}
