package server

import (
	"database/sql"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"harbor/internal/db"
)

// liveRequestSnapshot 返回进行中请求的副本（按开始时间倒序）。
func (s *Server) liveRequestSnapshot() []*RequestLog {
	s.recentMu.Lock()
	defer s.recentMu.Unlock()
	out := make([]*RequestLog, 0, 8)
	for i := len(s.recentOrder) - 1; i >= 0; i-- {
		log, ok := s.recent[s.recentOrder[i]]
		if !ok || !isLiveStatus(log.Status) {
			continue
		}
		cp := *log
		out = append(out, &cp)
	}
	return out
}

// filterLiveRequests 按管理台筛选条件过滤进行中请求。
func filterLiveRequests(logs []*RequestLog, search, status, providerName, modelName, keyID string, since, until time.Time) []*RequestLog {
	out := make([]*RequestLog, 0, len(logs))
	for _, log := range logs {
		if !matchesRequestFilter(log, search, status) {
			continue
		}
		if !since.IsZero() && log.QueuedAt.Before(since) {
			continue
		}
		if !until.IsZero() && !log.QueuedAt.Before(until) {
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
		out = append(out, log)
	}
	return out
}

func requestSortTime(log *RequestLog) time.Time {
	if !log.FinishedAt.IsZero() {
		return log.FinishedAt
	}
	return log.QueuedAt
}

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
	case "success", "error", "timeout", "cancelled", "queued", "connecting", "waiting", "responding":
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
		// DB 只有已结束记录；把内存中的进行中请求合并到第一页。
		live := filterLiveRequests(s.liveRequestSnapshot(), search, status, providerName, modelName, keyID, since, until)
		if len(live) > 0 {
			if offset == 0 {
				combined := make([]*RequestLog, 0, len(live)+len(out))
				combined = append(combined, live...)
				combined = append(combined, out...)
				seen := make(map[string]bool, len(combined))
				deduped := combined[:0]
				for _, item := range combined {
					if seen[item.RequestID] {
						continue
					}
					seen[item.RequestID] = true
					deduped = append(deduped, item)
				}
				sort.SliceStable(deduped, func(i, j int) bool {
					return requestSortTime(deduped[i]).After(requestSortTime(deduped[j]))
				})
				if len(deduped) > limit {
					deduped = deduped[:limit]
				}
				out = deduped
			}
			dbIDs := make(map[string]bool, len(recs))
			for _, rec := range recs {
				dbIDs[rec.RequestID] = true
			}
			for _, item := range live {
				if !dbIDs[item.RequestID] {
					total++
				}
			}
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
		if !since.IsZero() && requestSortTime(log).Before(since) {
			continue
		}
		if !until.IsZero() && !requestSortTime(log).Before(until) {
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
		cp := *log
		filtered = append(filtered, &cp)
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
	var snapshot *RequestLog
	if ok {
		cp := *log
		snapshot = &cp
	}
	s.recentMu.Unlock()
	if snapshot != nil {
		writeJSON(w, http.StatusOK, snapshot)
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
