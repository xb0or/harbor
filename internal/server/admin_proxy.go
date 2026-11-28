package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"harbor/internal/netproxy"
	"harbor/internal/provider"
)

const settingProxy = "proxy_config"

type proxySettings struct {
	Mode     string          `json:"mode"` // direct | proxy
	Active   string          `json:"active"`
	Nodes    []netproxy.Node `json:"nodes"`
	ClashURL string          `json:"clash_url"`
}

func (s *Server) loadProxyConfig() {
	s.proxyMu.Lock()
	defer s.proxyMu.Unlock()
	s.proxyCfg = proxySettings{Mode: "direct"}
	if s.store == nil {
		return
	}
	raw, err := s.store.GetSetting(settingProxy)
	if err != nil || strings.TrimSpace(raw) == "" {
		return
	}
	var cfg proxySettings
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		s.log.Warn("decode proxy config failed", "err", err.Error())
		return
	}
	s.proxyCfg = cfg
	s.applyProxyLocked()
}

func (s *Server) applyProxyLocked() {
	if s.proxyCfg.Mode != "proxy" {
		provider.SetProxyDialer(nil)
		return
	}
	for _, node := range s.proxyCfg.Nodes {
		if node.ID != s.proxyCfg.Active {
			continue
		}
		dial, err := netproxy.Dialer(node)
		if err != nil {
			s.log.Warn("apply proxy failed", "node", node.Name, "err", err.Error())
			provider.SetProxyDialer(nil)
			return
		}
		provider.SetProxyDialer(dial)
		s.log.Info("outbound proxy enabled", "node", node.Name, "type", node.Type)
		return
	}
	provider.SetProxyDialer(nil)
}

func (s *Server) proxyView() proxySettings {
	s.proxyMu.RLock()
	defer s.proxyMu.RUnlock()
	out := s.proxyCfg
	out.Nodes = append([]netproxy.Node(nil), s.proxyCfg.Nodes...)
	for i := range out.Nodes {
		if out.Nodes[i].Password != "" {
			out.Nodes[i].Password = "***"
		}
	}
	return out
}

func (s *Server) handleGetProxy(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.proxyView())
}

func (s *Server) handlePutProxy(w http.ResponseWriter, r *http.Request) {
	var req proxySettings
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		s.writeError(w, s.newRequestID(), invalidRequestErr(err.Error()))
		return
	}
	req.Mode = strings.ToLower(strings.TrimSpace(req.Mode))
	if req.Mode != "direct" && req.Mode != "proxy" {
		req.Mode = "direct"
	}
	seen := map[string]bool{}
	for i := range req.Nodes {
		node := &req.Nodes[i]
		node.Type = strings.ToLower(strings.TrimSpace(node.Type))
		if node.Type != "http" && node.Type != "socks5" {
			s.writeError(w, s.newRequestID(), invalidRequestErr("代理类型只能是 http 或 socks5"))
			return
		}
		node.Host = strings.TrimSpace(node.Host)
		if node.Host == "" || node.Port < 1 || node.Port > 65535 {
			s.writeError(w, s.newRequestID(), invalidRequestErr("代理地址或端口无效"))
			return
		}
		if node.ID == "" {
			node.ID = netproxy.NodeID(node.Type, node.Host, node.Port, node.Username)
		}
		if seen[node.ID] {
			s.writeError(w, s.newRequestID(), invalidRequestErr("代理节点 ID 重复"))
			return
		}
		seen[node.ID] = true
		if node.Password == "***" || node.Password == "" {
			if existing := s.proxyPassword(node.ID); existing != "" {
				node.Password = existing
			}
		}
	}
	if req.Mode == "proxy" {
		found := false
		for _, node := range req.Nodes {
			if node.ID == req.Active {
				found = true
				break
			}
		}
		if !found {
			s.writeError(w, s.newRequestID(), invalidRequestErr("启用代理时必须选择一个节点"))
			return
		}
	}
	s.proxyMu.Lock()
	s.proxyCfg = req
	s.applyProxyLocked()
	s.proxyMu.Unlock()
	if s.store != nil {
		if payload, err := json.Marshal(req); err == nil {
			if err := s.store.SetSetting(settingProxy, string(payload)); err != nil {
				s.log.Warn("persist proxy config failed", "err", err.Error())
			}
		}
	}
	writeJSON(w, http.StatusOK, s.proxyView())
}

func (s *Server) proxyPassword(id string) string {
	s.proxyMu.RLock()
	defer s.proxyMu.RUnlock()
	for _, node := range s.proxyCfg.Nodes {
		if node.ID == id {
			return node.Password
		}
	}
	return ""
}

func (s *Server) handleProxyClash(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL string `json:"url"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16))
	if err := dec.Decode(&req); err != nil {
		s.writeError(w, s.newRequestID(), invalidRequestErr(err.Error()))
		return
	}
	nodes, err := netproxy.FetchClash(r.Context(), req.URL, 20*time.Second)
	if err != nil {
		s.writeError(w, s.newRequestID(), invalidRequestErr(err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"nodes": nodes})
}
