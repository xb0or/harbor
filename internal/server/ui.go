package server

import (
	_ "embed"
	"net/http"
)

//go:embed ui/index.html
var indexHTML []byte

//go:embed ui/admin.html
var adminHTML []byte

//go:embed ui/admin.css
var adminCSS []byte

//go:embed ui/admin.js
var adminJS []byte

//go:embed ui/landing.js
var landingJS []byte

// registerUI 注册前端页面：
//   - GET /         → 公开落地页（index.html）
//   - GET /admin     → 管理台（admin.html），静态资源 /admin.css /admin.js 同源离线
//   - GET /admin.css → 管理台样式
//   - GET /admin.js  → 管理台脚本（登录先请求 /api/stats 校验 token）
func (s *Server) registerUI(mux *http.ServeMux) {
	mux.HandleFunc("GET /{$}", s.handleIndexPage)
	mux.HandleFunc("GET /admin", s.handleAdminPage)
	mux.HandleFunc("GET /admin.css", s.handleAdminCSS)
	mux.HandleFunc("GET /admin.js", s.handleAdminJS)
	mux.HandleFunc("GET /landing.js", s.handleLandingJS)
}

func (s *Server) handleIndexPage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(indexHTML)
}

func (s *Server) handleAdminPage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(adminHTML)
}

func (s *Server) handleAdminCSS(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/css; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(adminCSS)
}

func (s *Server) handleLandingJS(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(landingJS)
}

func (s *Server) handleAdminJS(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(adminJS)
}
