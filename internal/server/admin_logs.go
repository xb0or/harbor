package server

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"harbor/internal/logbuf"
)

// handleLogs GET /api/logs —— 最近系统日志（进程内环形缓冲）。
func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	level := strings.ToLower(strings.TrimSpace(q.Get("level")))
	switch level {
	case "debug", "info", "warn", "error":
	default:
		level = ""
	}
	limit := 200
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}
	var since time.Time
	if v := strings.TrimSpace(q.Get("since")); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			since = t
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"logs":  logbuf.Default.List(level, since, limit),
		"level": level,
		"limit": limit,
	})
}
