package server

import (
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"harbor/internal/db"
	"harbor/internal/provider"
)

func (s *Server) handleListProviders(w http.ResponseWriter, r *http.Request) {
	type credView struct {
		ID            string `json:"id"`
		Status        string `json:"status"`
		CooldownUntil string `json:"cooldown_until,omitempty"`
	}
	type provView struct {
		Name        string     `json:"name"`
		Type        string     `json:"type"`
		BaseURL     string     `json:"base_url"`
		Models      []string   `json:"models"`
		Concurrency int        `json:"concurrency"`
		RPM         int        `json:"rpm"`
		TPM         int64      `json:"tpm"`
		TPH         int64      `json:"tph"`
		TPD         int64      `json:"tpd"`
		Available   bool       `json:"available"`
		NextRecover string     `json:"next_recover,omitempty"`
		Credentials []credView `json:"credentials"`
	}
	out := make([]provView, 0, len(s.mgr.Providers()))
	for _, p := range s.mgr.Providers() {
		view := p.Snapshot()
		v := provView{
			Name: view.Name, Type: view.Type, BaseURL: view.BaseURL,
			Models: view.Models, Concurrency: view.Concurrency, RPM: view.RPM,
			TPM: view.TPM, TPH: view.TPH, TPD: view.TPD,
			Available: p.Available(),
		}
		v.NextRecover = p.NextRecover().Format(time.RFC3339)
		for _, c := range p.Credentials() {
			v.Credentials = append(v.Credentials, credView{ID: c.ID, Status: c.StatusLabel()})
		}
		out = append(out, v)
	}
	writeJSON(w, http.StatusOK, map[string]any{"providers": out})
}

func (s *Server) handleCreateProvider(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name        string   `json:"name"`
		Type        string   `json:"type"`
		BaseURL     string   `json:"base_url"`
		Models      []string `json:"models"`
		Credentials []string `json:"credentials"`
		Concurrency int      `json:"concurrency"`
		RPM         int      `json:"rpm"`
		TPM         int64    `json:"tpm"`
		TPH         int64    `json:"tph"`
		TPD         int64    `json:"tpd"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		s.writeError(w, s.newRequestID(), invalidRequestErr(err.Error()))
		return
	}
	if req.Name == "" || req.Type == "" || req.BaseURL == "" {
		s.writeError(w, s.newRequestID(), invalidRequestErr("name, type, base_url required"))
		return
	}
	if _, err := validatePublicProviderURL(req.BaseURL); err != nil {
		s.writeError(w, s.newRequestID(), invalidRequestErr(err.Error()))
		return
	}
	if _, exists := s.mgr.Get(req.Name); exists {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "provider already exists"})
		return
	}
	p := provider.New(req.Name, req.Type, req.BaseURL, req.Models, req.Credentials,
		s.cfg.Limits.RequestTimeoutDuration(), s.cfg.Limits.ProviderCooldownDuration(),
		req.Concurrency, req.RPM, req.TPM, req.TPH, req.TPD)
	// 管理台创建的上游走连接层 SSRF 防护（只连公网地址）。
	p.SetPublicOnly(true)
	s.mgr.AddProvider(p)
	s.persistProvider(p)
	for _, c := range p.Credentials() {
		s.persistSingleCredential(p.Name, c)
	}
	writeJSON(w, http.StatusOK, map[string]any{"name": req.Name, "created": true})
}

func (s *Server) handleUpdateProvider(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	p, ok := s.mgr.Get(name)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "provider not found"})
		return
	}
	var req struct {
		BaseURL     *string  `json:"base_url"`
		Models      []string `json:"models"`
		Concurrency *int     `json:"concurrency"`
		RPM         *int     `json:"rpm"`
		TPM         *int64   `json:"tpm"`
		TPH         *int64   `json:"tph"`
		TPD         *int64   `json:"tpd"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		s.writeError(w, s.newRequestID(), invalidRequestErr(err.Error()))
		return
	}
	if req.BaseURL != nil && *req.BaseURL != "" {
		if _, err := validatePublicProviderURL(*req.BaseURL); err != nil {
			s.writeError(w, s.newRequestID(), invalidRequestErr(err.Error()))
			return
		}
		p.UpdateBaseURL(*req.BaseURL)
		p.SetPublicOnly(true)
	}
	if req.Models != nil {
		p.UpdateModels(req.Models)
	}
	lim := p.Limits()
	if req.Concurrency != nil {
		lim.Concurrency = *req.Concurrency
	}
	if req.RPM != nil {
		lim.RPM = *req.RPM
	}
	if req.TPM != nil {
		lim.TPM = *req.TPM
	}
	if req.TPH != nil {
		lim.TPH = *req.TPH
	}
	if req.TPD != nil {
		lim.TPD = *req.TPD
	}
	p.UpdateLimits(lim.Concurrency, lim.RPM, lim.TPM, lim.TPH, lim.TPD)
	s.persistProvider(p)
	writeJSON(w, http.StatusOK, map[string]any{"name": name, "updated": true})
}

func (s *Server) handleDeleteProvider(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	routes := s.mgr.Routes()
	if !s.mgr.RemoveProvider(name) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "provider not found"})
		return
	}
	if s.store != nil {
		if err := s.store.DeleteProvider(name); err != nil {
			s.log.Warn("persist provider delete failed", "name", name, "err", err.Error())
		}
		for model, mappings := range routes {
			kept := make([]db.ModelMapping, 0, len(mappings))
			removed := false
			for _, mapping := range mappings {
				if mapping.Provider == name {
					removed = true
					continue
				}
				kept = append(kept, db.ModelMapping{Provider: mapping.Provider, UpstreamModel: mapping.UpstreamModel})
			}
			if !removed {
				continue
			}
			if len(kept) == 0 {
				if err := s.store.DeleteRoute(model); err != nil {
					s.log.Warn("persist route delete failed", "model", model, "err", err.Error())
				}
			} else if err := s.store.UpsertRoute(model, kept); err != nil {
				s.log.Warn("persist route update failed", "model", model, "err", err.Error())
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"name": name, "deleted": true})
}

func (s *Server) handleAddCredential(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	p, ok := s.mgr.Get(name)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "provider not found"})
		return
	}
	var req struct {
		Secret string `json:"secret"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		s.writeError(w, s.newRequestID(), invalidRequestErr(err.Error()))
		return
	}
	if req.Secret == "" {
		s.writeError(w, s.newRequestID(), invalidRequestErr("secret required"))
		return
	}
	c := p.AddCredential(req.Secret)
	if s.store != nil {
		s.persistSingleCredential(p.Name, c)
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": c.ID, "added": true})
}

func (s *Server) handleRemoveCredential(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	credID := r.PathValue("credID")
	p, ok := s.mgr.Get(name)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "provider not found"})
		return
	}
	if !p.RemoveCredential(credID) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "credential not found"})
		return
	}
	if s.store != nil {
		if err := s.store.DeleteCredential(name, credID); err != nil && err != sql.ErrNoRows {
			s.log.Warn("persist credential delete failed", "provider", name, "id", credID, "err", err.Error())
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": credID, "deleted": true})
}

func (s *Server) handleProbeModels(w http.ResponseWriter, r *http.Request) {
	var req struct {
		BaseURL    string `json:"base_url"`
		Credential string `json:"credential"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		s.writeError(w, s.newRequestID(), invalidRequestErr(err.Error()))
		return
	}
	if req.BaseURL == "" || req.Credential == "" {
		s.writeError(w, s.newRequestID(), invalidRequestErr("base_url and credential required"))
		return
	}
	baseURL, err := validatePublicProviderURL(req.BaseURL)
	if err != nil {
		s.writeError(w, s.newRequestID(), invalidRequestErr(err.Error()))
		return
	}
	endpoint := strings.TrimSuffix(baseURL.String(), "/") + "/models"
	httpReq, err := http.NewRequestWithContext(r.Context(), http.MethodGet, endpoint, nil)
	if err != nil {
		s.writeError(w, s.newRequestID(), invalidRequestErr(err.Error()))
		return
	}
	httpReq.Header.Set("Authorization", "Bearer "+req.Credential)
	resp, err := newSafeProbeClient(15 * time.Second).Do(httpReq)
	if err != nil {
		s.writeError(w, s.newRequestID(), invalidRequestErr("upstream request failed: "+err.Error()))
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		s.writeError(w, s.newRequestID(), invalidRequestErr("upstream returned "+strconv.Itoa(resp.StatusCode)))
		return
	}
	const maxProbeResponse = 1 << 20
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxProbeResponse+1))
	if err != nil {
		s.writeError(w, s.newRequestID(), invalidRequestErr("read upstream response failed"))
		return
	}
	if len(raw) > maxProbeResponse {
		s.writeError(w, s.newRequestID(), invalidRequestErr("upstream response too large"))
		return
	}
	var body struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		s.writeError(w, s.newRequestID(), invalidRequestErr("invalid upstream response"))
		return
	}
	models := make([]string, 0, len(body.Data))
	for _, m := range body.Data {
		if m.ID != "" {
			models = append(models, m.ID)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"models": models})
}
