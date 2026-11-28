package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"harbor/internal/db"
	"harbor/internal/provider"
)

// 模型管理接口：以「上游模型」为行，展示启用、并发系数、上下文、能力与映射关系。

// modelEntryView 是单行上游模型的完整视图。
type modelEntryView struct {
	ID           string   `json:"id"`
	Enabled      bool     `json:"enabled"`
	Context      int      `json:"context"`
	Capabilities []string `json:"capabilities"`
	// ConcurrencyCost 是该模型每次请求占用的并发槽系数（默认 1）。
	ConcurrencyCost float64  `json:"concurrency_cost"`
	MappedTo        []string `json:"mapped_to"`
	State           string   `json:"state"` // visible | mapped | disabled
}

// providerModelViews 汇总某个 Provider 的模型行（含反向映射）。
func (s *Server) providerModelViews(name string) ([]modelEntryView, bool) {
	p, ok := s.mgr.Get(name)
	if !ok {
		return nil, false
	}
	mappedTo := map[string][]string{}
	for unified, list := range s.mgr.Routes() {
		for _, m := range list {
			if m.Provider != name {
				continue
			}
			up := m.UpstreamModel
			if up == "" {
				up = unified
			}
			mappedTo[up] = append(mappedTo[up], unified)
		}
	}
	specs := p.ModelSpecs()
	out := make([]modelEntryView, 0, len(specs))
	for _, sp := range specs {
		v := modelEntryView{
			ID:              sp.ID,
			Enabled:         sp.IsEnabled(),
			Context:         sp.Context,
			Capabilities:    append([]string{}, sp.Capabilities...),
			ConcurrencyCost: sp.ConcurrencyCost,
			MappedTo:        append([]string{}, mappedTo[sp.ID]...),
		}
		if v.ConcurrencyCost <= 0 {
			v.ConcurrencyCost = 1
		}
		sort.Strings(v.MappedTo)
		switch {
		case !v.Enabled:
			v.State = "disabled"
		case len(v.MappedTo) > 0:
			v.State = "mapped"
		default:
			v.State = "visible"
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, true
}

// handleProviderModels GET /api/providers/{name}/models
func (s *Server) handleProviderModels(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	views, ok := s.providerModelViews(name)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "provider not found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"provider":     name,
		"capabilities": provider.ModelCaps,
		"models":       views,
	})
}

// handlePutProviderModels PUT /api/providers/{name}/models
// body: {"models":[{"id","enabled","context","capabilities","concurrency_cost"}]} 或 {"models":["id"]}
func (s *Server) handlePutProviderModels(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	p, ok := s.mgr.Get(name)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "provider not found"})
		return
	}
	var req struct {
		Models []json.RawMessage `json:"models"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		s.writeError(w, s.newRequestID(), invalidRequestErr(err.Error()))
		return
	}
	specs, err := parseModelSpecPayload(req.Models)
	if err != nil {
		s.writeError(w, s.newRequestID(), invalidRequestErr(err.Error()))
		return
	}
	p.SetModelSpecs(specs)
	s.persistProvider(p)
	views, _ := s.providerModelViews(name)
	writeJSON(w, http.StatusOK, map[string]any{"provider": name, "models": views})
}

// parseModelSpecPayload 同时接受字符串数组与对象数组。
func parseModelSpecPayload(raw []json.RawMessage) ([]provider.ModelSpec, error) {
	specs := make([]provider.ModelSpec, 0, len(raw))
	for _, item := range raw {
		item = bytes.TrimSpace(item)
		if len(item) == 0 || bytes.Equal(item, []byte("null")) {
			continue
		}
		if item[0] == '"' {
			var id string
			if err := json.Unmarshal(item, &id); err != nil {
				return nil, err
			}
			specs = append(specs, provider.ModelSpec{ID: id, Capabilities: []string{"chat"}})
			continue
		}
		var sp provider.ModelSpec
		if err := json.Unmarshal(item, &sp); err != nil {
			return nil, err
		}
		if math.IsNaN(sp.ConcurrencyCost) || math.IsInf(sp.ConcurrencyCost, 0) || sp.ConcurrencyCost < 0 || sp.ConcurrencyCost > 1000 {
			return nil, fmt.Errorf("model %q concurrency_cost must be between 0 and 1000", sp.ID)
		}
		if sp.ConcurrencyCost == 0 {
			sp.ConcurrencyCost = 1
		}
		if len(sp.Capabilities) == 0 {
			sp.Capabilities = []string{"chat"}
		}
		specs = append(specs, sp)
	}
	return provider.NormalizeSpecs(specs), nil
}

// handleProbeProviderModels POST /api/providers/{name}/models/probe
// 用已保存的凭证向上游 GET /models，返回上游实际提供的模型名（用于「未声明」比对）。
func (s *Server) handleProbeProviderModels(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	p, ok := s.mgr.Get(name)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "provider not found"})
		return
	}
	view := p.Snapshot()
	if _, err := validatePublicProviderURL(view.BaseURL); err != nil {
		s.writeError(w, s.newRequestID(), invalidRequestErr(err.Error()))
		return
	}
	cred := pickCredentialSecret(p)
	if cred == "" {
		s.writeError(w, s.newRequestID(), invalidRequestErr("provider has no credential"))
		return
	}
	endpoint := provider.JoinURL(view.BaseURL, "/models")
	httpReq, err := http.NewRequestWithContext(r.Context(), http.MethodGet, endpoint, nil)
	if err != nil {
		s.writeError(w, s.newRequestID(), invalidRequestErr(err.Error()))
		return
	}
	httpReq.Header.Set("Authorization", "Bearer "+cred)
	httpReq.Header.Set("Accept", "application/json")
	resp, err := newSafeProbeClient(15 * time.Second).Do(httpReq)
	if err != nil {
		s.writeError(w, s.newRequestID(), invalidRequestErr("upstream request failed: "+err.Error()))
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		s.writeError(w, s.newRequestID(), invalidRequestErr("upstream returned "+http.StatusText(resp.StatusCode)))
		return
	}
	const maxProbe = 1 << 20
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxProbe+1))
	if err != nil {
		s.writeError(w, s.newRequestID(), invalidRequestErr("read upstream response failed"))
		return
	}
	if len(raw) > maxProbe {
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
		if id := strings.TrimSpace(m.ID); id != "" {
			models = append(models, id)
		}
	}
	sort.Strings(models)
	declared := make([]string, 0, len(view.Specs))
	for _, sp := range view.Specs {
		declared = append(declared, sp.ID)
	}
	sort.Strings(declared)
	writeJSON(w, http.StatusOK, map[string]any{
		"provider": name,
		"models":   models,
		"declared": declared,
	})
}

// pickCredentialSecret 选一个可用凭证（优先 active）。
func pickCredentialSecret(p *provider.Provider) string {
	creds := p.Credentials()
	for _, c := range creds {
		if c.Status == provider.CredActive && strings.TrimSpace(c.Secret) != "" {
			return c.Secret
		}
	}
	for _, c := range creds {
		if strings.TrimSpace(c.Secret) != "" {
			return c.Secret
		}
	}
	return ""
}

// handleModelMap POST /api/model-map
// body: {"provider","upstream_model","unified","action":"add"|"remove"}
// 在统一模型路由上增删该上游模型的映射（一次只动一个 provider）。
func (s *Server) handleModelMap(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Provider string `json:"provider"`
		Upstream string `json:"upstream_model"`
		Unified  string `json:"unified"`
		Action   string `json:"action"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		s.writeError(w, s.newRequestID(), invalidRequestErr(err.Error()))
		return
	}
	req.Provider = strings.TrimSpace(req.Provider)
	req.Unified = strings.TrimSpace(req.Unified)
	req.Upstream = strings.TrimSpace(req.Upstream)
	action := strings.ToLower(strings.TrimSpace(req.Action))
	if action == "" {
		action = "add"
	}
	if action != "add" && action != "remove" {
		s.writeError(w, s.newRequestID(), invalidRequestErr("action must be add or remove"))
		return
	}
	if req.Provider == "" || req.Unified == "" {
		s.writeError(w, s.newRequestID(), invalidRequestErr("provider and unified are required"))
		return
	}
	if _, ok := s.mgr.Get(req.Provider); !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "provider not found"})
		return
	}
	if req.Upstream == "" {
		req.Upstream = req.Unified
	}

	existing := s.mgr.Routes()[req.Unified]
	next := make([]provider.ModelRoute, 0, len(existing)+1)
	replaced := false
	for _, m := range existing {
		if m.Provider != req.Provider {
			next = append(next, m)
			continue
		}
		replaced = true
		if action == "remove" {
			continue
		}
		next = append(next, provider.ModelRoute{Provider: req.Provider, UpstreamModel: req.Upstream})
	}
	if action == "add" && !replaced {
		next = append(next, provider.ModelRoute{Provider: req.Provider, UpstreamModel: req.Upstream})
	}
	if action == "remove" && !replaced {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "mapping not found"})
		return
	}

	if len(next) == 0 {
		s.mgr.RemoveRoute(req.Unified)
		if s.store != nil {
			if err := s.store.DeleteRoute(req.Unified); err != nil {
				s.log.Warn("persist route delete failed", "model", req.Unified, "err", err.Error())
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"unified": req.Unified, "mappings": []provider.ModelRoute{}, "removed": true})
		return
	}
	if !s.mgr.SetRoute(req.Unified, next) {
		s.writeError(w, s.newRequestID(), invalidRequestErr("provider missing"))
		return
	}
	if s.store != nil {
		mappings := make([]db.ModelMapping, len(next))
		for i, m := range next {
			mappings[i] = db.ModelMapping{Provider: m.Provider, UpstreamModel: m.UpstreamModel}
		}
		if err := s.store.UpsertRoute(req.Unified, mappings); err != nil {
			s.log.Warn("persist route failed", "model", req.Unified, "err", err.Error())
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"unified": req.Unified, "mappings": next})
}
