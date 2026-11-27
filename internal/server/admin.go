package server

import (
	"crypto/subtle"
	"fmt"
	"net/http"
	"strings"
	"time"

	"harbor/internal/db"
	"harbor/internal/key"
	"harbor/internal/provider"
)

// registerAdmin 注册管理接口（计划书 §16）。
// 需要 Authorization: Bearer <admin.token>。
func (s *Server) registerAdmin(mux *http.ServeMux) {
	if s.cfg.Admin.Token == "" {
		s.log.Info("admin API disabled (admin.token empty)")
		return
	}

	withAuth := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if !s.adminAuthorized(r) {
				s.writeError(w, s.newRequestID(), keyErrInvalidAdmin())
				return
			}
			h(w, r)
		}
	}

	mux.HandleFunc("GET /api/stats", withAuth(s.handleStats))
	mux.HandleFunc("GET /api/queue", withAuth(s.handleQueue))
	mux.HandleFunc("GET /api/requests", withAuth(s.handleListRequests))
	mux.HandleFunc("GET /api/requests/{id}", withAuth(s.handleRequestByID))
	mux.HandleFunc("GET /api/keys", withAuth(s.handleListKeys))
	mux.HandleFunc("POST /api/keys", withAuth(s.handleCreateKey))
	mux.HandleFunc("DELETE /api/keys/{id}", withAuth(s.handleDeleteKey))
	mux.HandleFunc("PUT /api/keys/{id}", withAuth(s.handleUpdateKey))
	mux.HandleFunc("POST /api/keys/{id}/ban", withAuth(s.handleBanKey))
	mux.HandleFunc("POST /api/keys/{id}/unban", withAuth(s.handleUnbanKey))
	mux.HandleFunc("GET /api/providers", withAuth(s.handleListProviders))
	mux.HandleFunc("POST /api/providers", withAuth(s.handleCreateProvider))
	mux.HandleFunc("POST /api/providers/probe-models", withAuth(s.handleProbeModels))
	mux.HandleFunc("GET /api/providers/{name}/models", withAuth(s.handleProviderModels))
	mux.HandleFunc("PUT /api/providers/{name}/models", withAuth(s.handlePutProviderModels))
	mux.HandleFunc("POST /api/providers/{name}/models/probe", withAuth(s.handleProbeProviderModels))
	mux.HandleFunc("POST /api/model-map", withAuth(s.handleModelMap))
	mux.HandleFunc("GET /api/model-catalog", withAuth(s.handleModelCatalog))
	mux.HandleFunc("GET /api/settings", withAuth(s.handleGetSettings))
	mux.HandleFunc("PUT /api/settings", withAuth(s.handlePutSettings))
	mux.HandleFunc("PUT /api/settings/keepalive", withAuth(s.handlePutKeepalive))
	mux.HandleFunc("PUT /api/providers/{name}", withAuth(s.handleUpdateProvider))
	mux.HandleFunc("DELETE /api/providers/{name}", withAuth(s.handleDeleteProvider))
	mux.HandleFunc("POST /api/providers/{name}/credentials", withAuth(s.handleAddCredential))
	mux.HandleFunc("DELETE /api/providers/{name}/credentials/{credID}", withAuth(s.handleRemoveCredential))
	mux.HandleFunc("GET /api/routes", withAuth(s.handleListRoutes))
	mux.HandleFunc("POST /api/routes", withAuth(s.handleSetRoute))
	mux.HandleFunc("DELETE /api/routes/{model}", withAuth(s.handleRemoveRoute))
}

func (s *Server) adminAuthorized(r *http.Request) bool {
	got := []byte(r.Header.Get("Authorization"))
	want := []byte("Bearer " + s.cfg.Admin.Token)
	if len(got) != len(want) {
		return false
	}
	return subtle.ConstantTimeCompare(got, want) == 1
}

func (s *Server) encryptionKey() ([]byte, error) {
	s.encOnce.Do(func() {
		if s.store == nil {
			return
		}
		s.encKey, s.encErr = db.LoadOrCreateKey(s.cfg.Storage.KeyPath)
	})
	if s.encErr != nil {
		return nil, s.encErr
	}
	if len(s.encKey) == 0 {
		return nil, fmt.Errorf("encryption key unavailable")
	}
	return s.encKey, nil
}

func requestLogFromDB(rec *db.Request) *RequestLog {
	if rec == nil {
		return nil
	}
	return &RequestLog{
		RequestID:         rec.RequestID,
		KeyID:             rec.KeyID,
		Model:             rec.Model,
		Provider:          rec.Provider,
		Status:            rec.Status,
		ErrorCode:         rec.ErrorCode,
		UpstreamCode:      rec.UpstreamCode,
		UpstreamType:      rec.UpstreamType,
		UpstreamHTTP:      rec.UpstreamHTTP,
		ErrorDetail:       rec.ErrorDetail,
		QueuedAt:          rec.QueuedAt,
		StartedAt:         rec.StartedAt,
		FinishedAt:        rec.FinishedAt,
		QueueWaitMs:       rec.QueueWaitMs,
		Attempts:          rec.Attempts,
		TokensIn:          rec.TokensIn,
		TokensOut:         rec.TokensOut,
		ClientUA:          rec.ClientUA,
		ClientIP:          rec.ClientIP,
		UpstreamModel:     rec.UpstreamModel,
		FirstTokenMs:      rec.FirstTokenMs,
		ResponseModel:     rec.ResponseModel,
		UpstreamRequestID: rec.UpstreamRequestID,
		CachedTokens:      rec.CachedTokens,
		CachedWriteTokens: rec.CachedWriteTokens,
		Stream:            rec.Stream,
	}
}

func matchesRequestFilter(log *RequestLog, q, status string) bool {
	if log == nil {
		return false
	}
	if status != "" && log.Status != status {
		return false
	}
	if q == "" {
		return true
	}
	q = strings.ToLower(q)
	return strings.Contains(strings.ToLower(log.RequestID), q) ||
		strings.Contains(strings.ToLower(log.KeyID), q) ||
		strings.Contains(strings.ToLower(log.Model), q) ||
		strings.Contains(strings.ToLower(log.Provider), q) ||
		strings.Contains(strings.ToLower(log.ErrorCode), q) ||
		strings.Contains(strings.ToLower(log.UpstreamCode), q) ||
		strings.Contains(strings.ToLower(log.UpstreamType), q) ||
		strings.Contains(strings.ToLower(log.ErrorDetail), q)
}

func (s *Server) persistKey(k *key.Key) {
	if err := s.persistKeySecret(k, ""); err != nil {
		s.log.Warn("persist key failed", "id", k.ID, "err", err.Error())
	}
}

func (s *Server) persistKeySecret(k *key.Key, secret string) error {
	if s.store == nil {
		return nil
	}
	secretEncrypted := ""
	if secret != "" {
		encKey, err := s.encryptionKey()
		if err != nil {
			return fmt.Errorf("load encryption key: %w", err)
		}
		secretEncrypted, err = db.Encrypt(encKey, secret)
		if err != nil {
			return fmt.Errorf("encrypt key secret: %w", err)
		}
	}
	rec := &db.Key{
		ID:              k.ID,
		Prefix:          k.Prefix,
		KeyHash:         k.Hash,
		SecretEncrypted: secretEncrypted,
		Status:          string(k.Status),
		AllowedModels:   k.AllowedModels,
		CreatedAt:       k.CreatedAt,
		ExpiresAt:       k.ExpiresAt,
	}
	return s.store.UpsertKey(rec)
}

func (s *Server) persistProvider(p *provider.Provider) {
	if s.store == nil {
		return
	}
	view := p.Snapshot()
	row := &db.ProviderRow{
		ID:          view.Name,
		Name:        view.Name,
		Type:        view.Type,
		BaseURL:     view.BaseURL,
		Models:      view.Models,
		ModelSpecs:  toDBModelSpecs(view.Specs),
		Timeout:     view.Timeout.String(),
		Cooldown:    view.Cooldown.String(),
		Concurrency: view.Concurrency,
		RPM:         view.RPM,
		TPM:         view.TPM,
		TPH:         view.TPH,
		TPD:         view.TPD,
		CreatedAt:   time.Now(),
	}
	if err := s.store.UpsertProvider(row); err != nil {
		s.log.Warn("persist provider failed", "name", view.Name, "err", err.Error())
	}
}

// toDBModelSpecs 把运行时模型声明转成持久化结构。
func toDBModelSpecs(specs []provider.ModelSpec) []db.ModelSpec {
	out := make([]db.ModelSpec, len(specs))
	for i, s := range specs {
		out[i] = db.ModelSpec{
			ID: s.ID, Enabled: s.Enabled, Visible: s.Visible,
			Context: s.Context, Capabilities: s.Capabilities,
		}
	}
	return out
}

// fromDBModelSpecs 把持久化模型声明还原为运行时结构。
func fromDBModelSpecs(specs []db.ModelSpec) []provider.ModelSpec {
	out := make([]provider.ModelSpec, len(specs))
	for i, s := range specs {
		out[i] = provider.ModelSpec{
			ID: s.ID, Enabled: s.Enabled, Visible: s.Visible,
			Context: s.Context, Capabilities: s.Capabilities,
		}
	}
	return out
}

func (s *Server) persistSingleCredential(providerName string, c *provider.Credential) {
	if s.store == nil {
		return
	}
	encKey, err := s.encryptionKey()
	if err != nil {
		s.log.Error("load enc key for credential", "provider", providerName, "err", err.Error())
		return
	}
	enc, err := db.Encrypt(encKey, c.Secret)
	if err != nil {
		s.log.Error("encrypt credential", "provider", providerName, "err", err.Error())
		return
	}
	if err := s.store.UpsertCredential(&db.Credential{
		ID:                c.ID,
		ProviderID:        providerName,
		SecretEncrypted:   enc,
		SecretFingerprint: credentialFingerprint(c.Secret),
		Status:            credStatusLabel(c),
		CooldownUntil:     c.CooldownUntil,
	}); err != nil {
		s.log.Warn("persist credential failed", "id", c.ID, "err", err.Error())
	}
}
