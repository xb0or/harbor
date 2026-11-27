package server

import (
	"encoding/json"
	"net/http"

	"harbor/internal/db"
	"harbor/internal/provider"
)

func (s *Server) handleListRoutes(w http.ResponseWriter, r *http.Request) {
	routes := s.mgr.Routes()
	type routeView struct {
		Model    string                `json:"model"`
		Mappings []provider.ModelRoute `json:"mappings"`
	}
	out := make([]routeView, 0, len(routes))
	for model, provs := range routes {
		out = append(out, routeView{Model: model, Mappings: provs})
	}
	writeJSON(w, http.StatusOK, map[string]any{"routes": out})
}

func (s *Server) handleSetRoute(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Model    string                `json:"model"`
		Mappings []provider.ModelRoute `json:"mappings"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		s.writeError(w, s.newRequestID(), invalidRequestErr(err.Error()))
		return
	}
	if req.Model == "" || len(req.Mappings) == 0 {
		s.writeError(w, s.newRequestID(), invalidRequestErr("model and mappings required"))
		return
	}
	if !s.mgr.SetRoute(req.Model, req.Mappings) {
		s.writeError(w, s.newRequestID(), invalidRequestErr("mappings reference unknown provider"))
		return
	}
	if s.store != nil {
		mappings := make([]db.ModelMapping, len(req.Mappings))
		for i, m := range req.Mappings {
			mappings[i] = db.ModelMapping{Provider: m.Provider, UpstreamModel: m.UpstreamModel}
		}
		if err := s.store.UpsertRoute(req.Model, mappings); err != nil {
			s.log.Warn("persist route failed", "model", req.Model, "err", err.Error())
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"model": req.Model, "set": true})
}

func (s *Server) handleRemoveRoute(w http.ResponseWriter, r *http.Request) {
	model := r.PathValue("model")
	if !s.mgr.RemoveRoute(model) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "route not found"})
		return
	}
	if s.store != nil {
		if err := s.store.DeleteRoute(model); err != nil {
			s.log.Warn("persist route delete failed", "model", model, "err", err.Error())
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"model": model, "deleted": true})
}
