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
//   - GET /           → 公开落地页（index.html）
//   - GET /{admin}/   → 管理台（路径可在设置页热改，页面内资源相对同路径）
//   - GET /landing.js → 落地页脚本
func (s *Server) registerUI(mux *http.ServeMux) {
	mux.HandleFunc("GET /{$}", s.handleIndexPage)
	mux.HandleFunc("GET /landing.js", s.handleLandingJS)
	// 管理台入口不固定：由 handleDynamicUI 按当前 adminPath 分发。
	mux.HandleFunc("GET /{path...}", s.handleDynamicUI)
}

// handleDynamicUI 处理落地页以外的 GET：
// 只有当前管理台路径及其相对资源可见，旧路径立即 404。
func (s *Server) handleDynamicUI(w http.ResponseWriter, r *http.Request) {
	admin := s.AdminPath()
	path := r.URL.Path
	switch path {
	case admin:
		http.Redirect(w, r, admin+"/", http.StatusTemporaryRedirect)
	case admin + "/":
		s.handleAdminPage(w, r)
	case admin + "/admin.css":
		s.handleAdminCSS(w, r)
	case admin + "/admin.js":
		s.handleAdminJS(w, r)
	default:
		http.NotFound(w, r)
	}
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
