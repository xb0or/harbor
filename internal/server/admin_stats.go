package server

import (
	"net/http"
	"time"
)

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	sq := s.sched.Stats()
	st := s.stats.Snapshot()
	resp := map[string]any{
		"now": time.Now(),
		"queue": map[string]any{
			"depth":        sq.Depth,
			"max_depth":    sq.MaxDepth,
			"running":      sq.Running,
			"longest_wait": sq.LongestWait.String(),
		},
		"today": map[string]any{
			"requests":      st.Requests,
			"success":       st.Success,
			"failed":        st.Failed,
			"queued":        st.Queued,
			"tokens_in":     st.TokensIn,
			"tokens_out":    st.TokensOut,
			"tokens_cached": st.TokensCached,
			"tokens_total":  st.TokensTotal,
			"p50_wait":      st.P50WaitMs,
			"p95_wait":      st.P95WaitMs,
			"max_wait_ms":   st.MaxWaitMs,
		},
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleQueue(w http.ResponseWriter, r *http.Request) {
	waiters := s.sched.Waiters()
	type item struct {
		RequestID  string `json:"request_id"`
		Model      string `json:"model"`
		Position   int    `json:"position"`
		QueuedAt   string `json:"queued_at"`
		WaitingFor string `json:"waiting_for"`
	}
	out := make([]item, 0, len(waiters))
	for _, j := range waiters {
		out = append(out, item{
			RequestID:  j.ID,
			Model:      j.Model,
			Position:   j.Position,
			QueuedAt:   j.EnqueuedAt.Format(time.RFC3339),
			WaitingFor: j.Wait.Round(time.Millisecond).String(),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"depth":     len(out),
		"max_depth": s.sched.Stats().MaxDepth,
		"items":     out,
	})
}
