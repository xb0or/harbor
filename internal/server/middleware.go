package server

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strings"
	"time"

	"harbor/internal/normalize"
)

// statusRecorder 记录响应状态码与字节数。
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// recoverMiddleware panic → 统一 500 internal_error（收敛格式）。
func (s *Server) recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.log.Error("panic recovered", "panic", rec, "path", r.URL.Path)
				normalize.Write(w, normalize.ErrInternal, "")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// logMiddleware 请求日志（debug 级，避免刷屏）。
func (s *Server) logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)
		if rec.status == 0 {
			rec.status = http.StatusOK
		}
		s.log.Debug("http",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"bytes", rec.bytes,
			"dur", time.Since(start).Round(time.Millisecond).String(),
			"remote", r.RemoteAddr,
			"request_id", w.Header().Get("X-Request-Id"),
		)
	})
}

func (s *Server) securityHeadersMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Permissions-Policy", "camera=(), geolocation=(), microphone=()")
		h.Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		if strings.HasPrefix(r.URL.Path, "/api/") || r.URL.Path == "/" || strings.HasPrefix(r.URL.Path, "/admin") {
			h.Set("Cache-Control", "no-store")
		}
		if strings.HasPrefix(r.URL.Path, "/admin") {
			// 管理台离线同源：禁止 inline style/script。进度条走 <progress>，布局走 class。
			h.Set("Content-Security-Policy", "default-src 'self'; img-src 'self' data:; style-src 'self'; script-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'")
		}
		next.ServeHTTP(w, r)
	})
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// keyErrInvalidAdmin 管理凭证无效 → 401 收敛错误。
func keyErrInvalidAdmin() *normalize.Error {
	return normalize.New(normalize.ErrInvalidAPIKey, "invalid admin token")
}

func invalidRequestErr(reason string) *normalize.Error {
	e := normalize.New(normalize.ErrInvalidRequest, reason)
	// 管理端点由管理员排障使用：把具体原因带回前端，而不是只显示通用的 Invalid request。
	if reason != "" {
		e.Message = reason
	}
	return e
}
