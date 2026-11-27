package server

import (
	"net/http"
	"time"
)

// registerPublic 注册无需鉴权的公开状态接口（首页实时状态带使用）。
// 安全边界（计划书 §08/§18）：只暴露聚合数字，不暴露供应商名称、
// Base URL、凭证、Harbor Key、模型清单与任何原始错误信息。
func (s *Server) registerPublic(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/public/status", s.handlePublicStatus)
}

// handlePublicStatus 返回落地页所需的聚合快照（无需管理凭证）。
func (s *Server) handlePublicStatus(w http.ResponseWriter, r *http.Request) {
	sq := s.sched.Stats()
	st := s.stats.Snapshot()

	total := len(s.mgr.Providers())
	available := 0
	for _, p := range s.mgr.Providers() {
		if p.Available() {
			available++
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"service":  "harbor",
		"tagline":  "一个不急着报 429 的 AI 网关",
		"now":      time.Now().UTC(),
		"uptime_s": int64(time.Since(st.StartTime).Seconds()),
		"queue": map[string]any{
			"depth":           sq.Depth,
			"max_depth":       sq.MaxDepth,
			"running":         sq.Running,
			"longest_wait_ms": sq.LongestWait.Milliseconds(),
		},
		"today": map[string]any{
			"requests":     st.Requests,
			"success":      st.Success,
			"failed":       st.Failed,
			"queued":       st.Queued,
			"tokens_in":    st.TokensIn,
			"tokens_out":   st.TokensOut,
			"tokens_total": st.TokensTotal,
			"p50_wait_ms":  st.P50WaitMs,
			"p95_wait_ms":  st.P95WaitMs,
			"max_wait_ms":  st.MaxWaitMs,
		},
		// 仅数量级信息，不含名称与地址
		"providers": map[string]any{"total": total, "available": available},
		"models":    map[string]any{"total": len(s.mgr.PublicModels())},
	})
}
