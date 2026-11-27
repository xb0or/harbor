package server

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"sort"
	"time"

	"harbor/internal/normalize"
)

func (s *Server) handleListKeys(w http.ResponseWriter, r *http.Request) {
	keys := s.keys.List()
	sort.Slice(keys, func(i, j int) bool { return keys[i].CreatedAt.Before(keys[j].CreatedAt) })
	type outKey struct {
		ID            string   `json:"id"`
		Prefix        string   `json:"prefix"`
		Status        string   `json:"status"`
		AllowedModels []string `json:"allowed_models"`
		CreatedAt     string   `json:"created_at"`
	}
	out := make([]outKey, 0, len(keys))
	for _, k := range keys {
		out = append(out, outKey{
			ID: k.ID, Prefix: k.Prefix, Status: string(k.Status),
			AllowedModels: k.AllowedModels,
			CreatedAt:     k.CreatedAt.Format(time.RFC3339),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"keys": out})
}

func (s *Server) handleCreateKey(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AllowedModels []string `json:"allowed_models"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		s.writeError(w, s.newRequestID(), invalidRequestErr(err.Error()))
		return
	}
	secret := "hr-" + randomHex(16)
	k := s.keys.Add(secret, req.AllowedModels)
	if err := s.persistKeySecret(k, secret); err != nil {
		s.log.Error("persist key failed", "id", k.ID, "err", err.Error())
		s.keys.Delete(k.ID)
		s.writeError(w, s.newRequestID(), normalize.New(normalize.ErrInternal, "persist key failed"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id":     k.ID,
		"prefix": k.Prefix,
		"secret": secret,
	})
}

func (s *Server) handleDeleteKey(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.keys.Delete(id) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "key not found"})
		return
	}
	if s.store != nil {
		if err := s.store.DeleteKey(id); err != nil {
			s.log.Warn("persist key delete failed", "id", id, "err", err.Error())
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "deleted": true})
}

func (s *Server) handleUpdateKey(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req struct {
		AllowedModels []string `json:"allowed_models"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		s.writeError(w, s.newRequestID(), invalidRequestErr(err.Error()))
		return
	}
	if !s.keys.Update(id, req.AllowedModels) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "key not found"})
		return
	}
	if k, ok := s.keys.Get(id); ok && s.store != nil {
		s.persistKey(k)
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "updated": true})
}

func (s *Server) handleBanKey(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.keys.Ban(id) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "key not found"})
		return
	}
	if s.store != nil {
		if err := s.store.SetKeyStatus(id, "banned"); err != nil && err != sql.ErrNoRows {
			s.log.Warn("persist key ban failed", "id", id, "err", err.Error())
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "status": "banned"})
}

func (s *Server) handleUnbanKey(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.keys.Unban(id) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "key not found"})
		return
	}
	if s.store != nil {
		if err := s.store.SetKeyStatus(id, "active"); err != nil && err != sql.ErrNoRows {
			s.log.Warn("persist key unban failed", "id", id, "err", err.Error())
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "status": "active"})
}
