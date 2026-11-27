package server

import (
	"database/sql"
	"net/http"
	"strconv"
	"strings"
	"time"

	"harbor/internal/db"
)

// handleListRequests GET /api/requests
// 支持：q / status / provider / model / key_id / since（RFC3339 或 YYYY-MM-DD）/ limit / offset
func (s *Server) handleListRequests(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := 50
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 500 {
			limit = n
		}
	}
	offset := 0
	if v := q.Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}
	search := strings.TrimSpace(q.Get("q"))
	status := strings.TrimSpace(q.Get("status"))
	switch status {
	case "success", "error", "timeout", "cancelled":
	default:
		status = ""
	}
	providerName := strings.TrimSpace(q.Get("provider"))
	modelName := strings.TrimSpace(q.Get("model"))
	keyID := strings.TrimSpace(q.Get("key_id"))

	parseTime := func(v string) time.Time {
		v = strings.TrimSpace(v)
		if v == "" {
			return time.Time{}
		}
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			return t
		}
		if t, err := time.Parse("2006-01-02", v); err == nil {
			return t
		}
		return time.Time{}
	}
	since := parseTime(q.Get("since"))
	until := parseTime(q.Get("until"))

	if s.store != nil {
		recs, total, err := s.store.ListRequests(db.RequestFilter{
			Q: search, Status: status, Provider: providerName, Model: modelName, KeyID: keyID,
			Since: since, Until: until, Limit: limit, Offset: offset,
		})
		if err != nil {
			s.log.Warn("list requests failed", "err", err.Error())
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
			return
		}
		out := make([]*RequestLog, 0, len(recs))
		for _, rec := range recs {
			out = append(out, requestLogFromDB(rec))
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"requests": out,
			"total":    total,
			"limit":    limit,
			"offset":   offset,
		})
		return
	}

	s.recentMu.Lock()
	filtered := make([]*RequestLog, 0, len(s.recentOrder))
	for i := len(s.recentOrder) - 1; i >= 0; i-- {
		log, ok := s.recent[s.recentOrder[i]]
		if !ok || !matchesRequestFilter(log, search, status) {
			continue
		}
		if !since.IsZero() && log.FinishedAt.Before(since) {
			continue
		}
		if !until.IsZero() && !log.FinishedAt.Before(until) {
			continue
		}
		if providerName != "" && log.Provider != providerName {
			continue
		}
		if modelName != "" && log.Model != modelName {
			continue
		}
		if keyID != "" && log.KeyID != keyID {
			continue
		}
		filtered = append(filtered, log)
	}
	s.recentMu.Unlock()
	total := len(filtered)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"requests": filtered[offset:end],
		"total":    total,
		"limit":    limit,
		"offset":   offset,
	})
}

// handleRequestByID GET /api/requests/{id}
func (s *Server) handleRequestByID(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.recentMu.Lock()
	log, ok := s.recent[id]
	s.recentMu.Unlock()
	if ok {
		writeJSON(w, http.StatusOK, log)
		return
	}
	if s.store != nil {
		rec, err := s.store.GetRequest(id)
		if err == nil {
			writeJSON(w, http.StatusOK, rec)
			return
		}
		if err != sql.ErrNoRows {
			s.log.Warn("query persisted request", "request_id", id, "err", err.Error())
		}
	}
	writeJSON(w, http.StatusNotFound, map[string]string{"error": "request not found"})
}
