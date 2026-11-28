package server

// 启动时把持久化配置（Harbor Keys / Provider / 路由 / 凭证）恢复到内存。
// 有 db_path 时 SQLite 是运行时权威；harbor.yaml 只给空域做种子。
// 纯内存模式仍始终加载 yaml。

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"harbor/internal/config"
	"harbor/internal/db"
	"harbor/internal/key"
	"harbor/internal/provider"
)

const placeholderCredential = "sk-REPLACE_ME"

// RestoreStartup 将 SQLite 中的运行时配置恢复到内存组件。
// 调用方应传入空的 keyStore / Manager；yaml 不得先灌内存。
// 无密文且无法恢复的旧 yaml Key 会从库中清掉，避免挡住空域种子。
// 无法解密的旧凭证/Key 只记录告警并跳过，避免阻断整个网关启动。
func RestoreStartup(store *db.DB, keyStore *key.Store, mgr *provider.Manager, timeout, cooldown time.Duration, encKey []byte, log *slog.Logger) error {
	if store == nil {
		return nil
	}
	if keyStore == nil || mgr == nil {
		return fmt.Errorf("restore requires key store and provider manager")
	}
	if log == nil {
		log = slog.Default()
	}

	persistedKeys, err := store.ListKeys()
	if err != nil {
		return fmt.Errorf("list persisted keys: %w", err)
	}
	for _, row := range persistedKeys {
		if row.SecretEncrypted == "" {
			// 仅哈希、内存里已有（测试/旧路径）时覆盖元数据；否则是 yaml 灌库的幽灵 Key，丢掉。
			if keyStore.RestoreMetadata(row.KeyHash, row.ID, row.Prefix, restoreKeyStatus(row.Status), row.AllowedModels, row.CreatedAt, row.ExpiresAt) {
				continue
			}
			if err := store.DeleteKey(row.ID); err != nil {
				log.Warn("drop unrestorable key failed", "id", row.ID, "err", err.Error())
			} else {
				log.Warn("dropped unrestorable key", "id", row.ID)
			}
			continue
		}
		secret, err := db.Decrypt(encKey, row.SecretEncrypted)
		if err != nil {
			log.Warn("restore key failed", "id", row.ID, "err", err.Error())
			continue
		}
		if key.Hash(secret) != row.KeyHash {
			log.Warn("restore key hash mismatch", "id", row.ID)
			continue
		}
		keyStore.Restore(secret, row.ID, row.Prefix, restoreKeyStatus(row.Status), row.AllowedModels, row.CreatedAt, row.ExpiresAt)
	}

	persistedProviders, err := store.ListProviders()
	if err != nil {
		return fmt.Errorf("list persisted providers: %w", err)
	}
	for _, row := range persistedProviders {
		if row.Name == "" {
			continue
		}
		p, ok := mgr.Get(row.Name)
		pt := persistedDuration(row.Timeout, timeout)
		pc := persistedDuration(row.Cooldown, cooldown)
		if !ok {
			p = provider.New(row.Name, row.Type, row.BaseURL, row.Models, nil, pt, pc,
				row.Concurrency, row.RPM, row.TPM, row.TPH, row.TPD)
			applyProviderSpecs(p, row)
			p.UpdateHeaders(row.Headers, row.PassHeaders)
			mgr.AddProvider(p)
			continue
		}
		if row.Type != "" {
			p.UpdateType(row.Type)
		}
		p.UpdateBaseURL(row.BaseURL)
		applyProviderSpecs(p, row)
		p.UpdateTimeoutCooldown(pt, pc)
		p.UpdateLimits(row.Concurrency, row.RPM, row.TPM, row.TPH, row.TPD)
		p.UpdateHeaders(row.Headers, row.PassHeaders)
	}

	persistedCredentials, err := store.ListCredentials()
	if err != nil {
		return fmt.Errorf("list persisted credentials: %w", err)
	}
	for _, row := range persistedCredentials {
		p, ok := mgr.Get(row.ProviderID)
		if !ok {
			log.Warn("restore credential skipped: provider missing", "id", row.ID, "provider", row.ProviderID)
			continue
		}
		secret, err := db.Decrypt(encKey, row.SecretEncrypted)
		if err != nil {
			log.Warn("restore credential failed", "id", row.ID, "provider", row.ProviderID, "err", err.Error())
			continue
		}
		if row.SecretFingerprint != "" && !strings.HasPrefix(row.SecretFingerprint, "legacy:") && credentialFingerprint(secret) != row.SecretFingerprint {
			log.Warn("restore credential fingerprint mismatch", "id", row.ID, "provider", row.ProviderID)
			continue
		}
		if strings.HasPrefix(row.SecretFingerprint, "legacy:") {
			if err := store.UpdateCredentialFingerprint(row.ProviderID, row.ID, credentialFingerprint(secret)); err != nil {
				log.Warn("upgrade credential fingerprint failed", "id", row.ID, "provider", row.ProviderID, "err", err.Error())
			}
		}
		p.RestoreCredential(row.ID, secret, restoreCredentialStatus(row.Status), row.CooldownUntil)
	}

	persistedRoutes, err := store.ListRoutes()
	if err != nil {
		return fmt.Errorf("list persisted routes: %w", err)
	}
	for _, row := range persistedRoutes {
		if len(row.Mappings) == 0 {
			continue
		}
		mappings := make([]provider.ModelRoute, len(row.Mappings))
		for i, mapping := range row.Mappings {
			mappings[i] = provider.ModelRoute{Provider: mapping.Provider, UpstreamModel: mapping.UpstreamModel}
		}
		if !mgr.SetRoute(row.Model, mappings) {
			log.Warn("restore route skipped: provider missing", "model", row.Model)
		}
	}
	return nil
}

// applyProviderSpecs 用持久化的模型声明覆盖 Provider；旧库（只有 models 名）走 UpdateModels。
func applyProviderSpecs(p *provider.Provider, row *db.ProviderRow) {
	if len(row.ModelSpecs) > 0 {
		p.SetModelSpecs(fromDBModelSpecs(row.ModelSpecs))
		return
	}
	p.UpdateModels(row.Models)
}

func persistedDuration(raw string, fallback time.Duration) time.Duration {
	if d, err := time.ParseDuration(raw); err == nil && d > 0 {
		return d
	}
	return fallback
}

func restoreKeyStatus(raw string) key.Status {
	switch key.Status(raw) {
	case key.StatusBanned, key.StatusExpired:
		return key.Status(raw)
	default:
		return key.StatusActive
	}
}

func restoreCredentialStatus(raw string) provider.CredStatus {
	if raw == "disabled" {
		return provider.CredDisabled
	}
	if raw == "cooling" || strings.HasPrefix(raw, "cooling_until=") {
		return provider.CredCooling
	}
	return provider.CredActive
}

// MirrorStartup 将启动配置中的 Key 元数据与加密后的上游凭证写入 store.
// 纯内存模式（store == nil）为空操作；单个失败只记日志，不中断启动。
// 设计：SQLite 是持久化镜像，内存组件（key.Store / provider）是运行时权威。
func MirrorStartup(store *db.DB, keyStore *key.Store, providers []*provider.Provider, encKey []byte, log *slog.Logger) {
	if store == nil || log == nil {
		return
	}
	for _, k := range keyStore.List() {
		if err := store.UpsertKey(&db.Key{
			ID: k.ID, Prefix: k.Prefix, KeyHash: k.Hash, Status: string(k.Status),
			AllowedModels: k.AllowedModels,
			CreatedAt:     k.CreatedAt, ExpiresAt: k.ExpiresAt,
		}); err != nil {
			log.Warn("mirror key failed", "id", k.ID, "err", err.Error())
		}
	}
	for i, p := range providers {
		view := p.Snapshot()
		// 镜像 Provider 元数据到 providers 表
		if err := store.UpsertProvider(&db.ProviderRow{
			ID: view.Name, Name: view.Name, Type: view.Type, BaseURL: view.BaseURL,
			Models:      view.Models,
			ModelSpecs:  toDBModelSpecs(view.Specs),
			Timeout:     view.Timeout.String(),
			Cooldown:    view.Cooldown.String(),
			Concurrency: view.Concurrency, RPM: view.RPM,
			TPM: view.TPM, TPH: view.TPH, TPD: view.TPD,
			SortOrder: i, Headers: view.Headers, PassHeaders: view.PassHeaders,
			CreatedAt: time.Now(),
		}); err != nil {
			log.Warn("mirror provider failed", "name", view.Name, "err", err.Error())
		}
		// 镜像凭证到 provider_credentials 表
		for _, c := range p.Credentials() {
			enc, err := db.Encrypt(encKey, c.Secret)
			if err != nil {
				log.Error("encrypt credential", "provider", view.Name, "credential", c.ID, "err", err.Error())
				continue
			}
			if err := store.UpsertCredential(&db.Credential{
				ID:                c.ID,
				ProviderID:        view.Name,
				SecretEncrypted:   enc,
				SecretFingerprint: credentialFingerprint(c.Secret),
				Status:            credStatusLabel(c),
				CooldownUntil:     c.CooldownUntil,
			}); err != nil {
				log.Warn("mirror credential failed", "id", c.ID, "err", err.Error())
			}
		}
	}
}

// credentialFingerprint 计算明文 secret 的稳定指纹（幂等镜像去重键）。
func credentialFingerprint(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

// credStatusLabel 把凭证运行时状态映射为稳定文本（SQLite 镜像用）。
func credStatusLabel(c *provider.Credential) string {
	switch c.Status {
	case provider.CredCooling:
		return "cooling"
	case provider.CredDisabled:
		return "disabled"
	default:
		return "active"
	}
}

// MirrorMappings 将启动配置中的模型映射写入 store。
// 纯内存模式（store == nil）为空操作；单个失败只记日志，不中断启动。
func MirrorMappings(store *db.DB, mappings map[string][]provider.ModelRoute, log *slog.Logger) {
	if store == nil || log == nil {
		return
	}
	for model, routes := range mappings {
		dbMappings := make([]db.ModelMapping, len(routes))
		for i, r := range routes {
			dbMappings[i] = db.ModelMapping{Provider: r.Provider, UpstreamModel: r.UpstreamModel}
		}
		if err := store.UpsertRoute(model, dbMappings); err != nil {
			log.Warn("mirror route failed", "model", model, "err", err.Error())
		}
	}
}

// SeedEmptyFromConfig 按域独立把 yaml 灌进空库。任一域已有数据则跳过该域。
// 占位凭证 sk-REPLACE_ME 不进库。Key 必须加密 secret 后 persist，否则重启无法认证。
func SeedEmptyFromConfig(store *db.DB, cfg *config.Config, keyStore *key.Store, mgr *provider.Manager, timeout, cooldown time.Duration, encKey []byte, log *slog.Logger) error {
	if store == nil || cfg == nil {
		return nil
	}
	if keyStore == nil || mgr == nil {
		return fmt.Errorf("seed requires key store and provider manager")
	}
	if log == nil {
		log = slog.Default()
	}

	keys, err := store.ListKeys()
	if err != nil {
		return fmt.Errorf("list keys for seed: %w", err)
	}
	if len(keys) == 0 {
		for _, kc := range cfg.Keys {
			if strings.TrimSpace(kc.Secret) == "" || strings.TrimSpace(kc.Secret) == placeholderCredential {
				continue
			}
			k := keyStore.Add(kc.Secret, kc.AllowedModels)
			if err := persistSeedKey(store, k, kc.Secret, encKey); err != nil {
				log.Warn("seed key failed", "id", k.ID, "err", err.Error())
				keyStore.Delete(k.ID)
				continue
			}
			log.Info("key seeded", "id", k.ID)
		}
	}

	providers, err := store.ListProviders()
	if err != nil {
		return fmt.Errorf("list providers for seed: %w", err)
	}
	if len(providers) == 0 {
		for _, pc := range cfg.Providers {
			secrets := usableCredentials(pc.Credentials)
			if len(secrets) == 0 {
				log.Info("skip placeholder provider", "name", pc.Name)
				continue
			}
			p := provider.New(
				pc.Name, pc.Type, pc.BaseURL, pc.Models, secrets,
				timeout, cooldown,
				pc.Concurrency, pc.RPM, pc.TPM, pc.TPH, pc.TPD,
			)
			p.UpdateHeaders(pc.Headers, pc.PassHeaders)
			mgr.AddProvider(p)
			if err := persistSeedProvider(store, p, encKey, log); err != nil {
				log.Warn("seed provider failed", "name", pc.Name, "err", err.Error())
				mgr.RemoveProvider(pc.Name)
				continue
			}
			log.Info("provider seeded", "name", pc.Name, "base_url", pc.BaseURL, "credentials", len(secrets))
		}
	}

	routes, err := store.ListRoutes()
	if err != nil {
		return fmt.Errorf("list routes for seed: %w", err)
	}
	if len(routes) == 0 {
		for model, mappings := range yamlRoutes(cfg) {
			if !mgr.SetRoute(model, mappings) {
				log.Warn("seed route skipped: provider missing", "model", model)
				continue
			}
			dbMappings := make([]db.ModelMapping, len(mappings))
			for i, r := range mappings {
				dbMappings[i] = db.ModelMapping{Provider: r.Provider, UpstreamModel: r.UpstreamModel}
			}
			if err := store.UpsertRoute(model, dbMappings); err != nil {
				log.Warn("seed route failed", "model", model, "err", err.Error())
			} else {
				log.Info("route seeded", "model", model)
			}
		}
	}
	return nil
}

func persistSeedKey(store *db.DB, k *key.Key, secret string, encKey []byte) error {
	enc, err := db.Encrypt(encKey, secret)
	if err != nil {
		return err
	}
	return store.UpsertKey(&db.Key{
		ID: k.ID, Prefix: k.Prefix, KeyHash: k.Hash,
		SecretEncrypted: enc, Status: string(k.Status),
		AllowedModels: k.AllowedModels,
		CreatedAt:     k.CreatedAt, ExpiresAt: k.ExpiresAt,
	})
}

func persistSeedProvider(store *db.DB, p *provider.Provider, encKey []byte, log *slog.Logger) error {
	view := p.Snapshot()
	if err := store.UpsertProvider(&db.ProviderRow{
		ID: view.Name, Name: view.Name, Type: view.Type, BaseURL: view.BaseURL,
		Models:      view.Models,
		ModelSpecs:  toDBModelSpecs(view.Specs),
		Timeout:     view.Timeout.String(),
		Cooldown:    view.Cooldown.String(),
		Concurrency: view.Concurrency, RPM: view.RPM,
		TPM: view.TPM, TPH: view.TPH, TPD: view.TPD,
		Headers: view.Headers, PassHeaders: view.PassHeaders,
		CreatedAt: time.Now(),
	}); err != nil {
		return err
	}
	for _, c := range p.Credentials() {
		enc, err := db.Encrypt(encKey, c.Secret)
		if err != nil {
			log.Error("encrypt credential", "provider", view.Name, "credential", c.ID, "err", err.Error())
			continue
		}
		if err := store.UpsertCredential(&db.Credential{
			ID:                c.ID,
			ProviderID:        view.Name,
			SecretEncrypted:   enc,
			SecretFingerprint: credentialFingerprint(c.Secret),
			Status:            credStatusLabel(c),
			CooldownUntil:     c.CooldownUntil,
		}); err != nil {
			log.Warn("seed credential failed", "id", c.ID, "err", err.Error())
		}
	}
	return nil
}

func usableCredentials(secrets []string) []string {
	out := make([]string, 0, len(secrets))
	for _, s := range secrets {
		s = strings.TrimSpace(s)
		if s == "" || s == placeholderCredential {
			continue
		}
		out = append(out, s)
	}
	return out
}

func yamlRoutes(cfg *config.Config) map[string][]provider.ModelRoute {
	mappings := map[string][]provider.ModelRoute{}
	if cfg == nil {
		return mappings
	}
	for model, route := range cfg.Routes {
		if len(route.Mappings) > 0 {
			for _, m := range route.Mappings {
				mappings[model] = append(mappings[model], provider.ModelRoute{
					Provider: m.Provider, UpstreamModel: m.UpstreamModel,
				})
			}
			continue
		}
		for _, p := range route.Providers {
			mappings[model] = append(mappings[model], provider.ModelRoute{Provider: p})
		}
	}
	return mappings
}

// LoadYAMLRuntime 纯内存模式：始终把 yaml keys/providers/routes 灌进内存。
func LoadYAMLRuntime(cfg *config.Config, keyStore *key.Store, log *slog.Logger) *provider.Manager {
	if cfg == nil {
		return provider.NewManager(nil, nil)
	}
	if keyStore == nil {
		keyStore = key.NewStore()
	}
	if log == nil {
		log = slog.Default()
	}
	for _, kc := range cfg.Keys {
		if strings.TrimSpace(kc.Secret) == "" {
			continue
		}
		k := keyStore.Add(kc.Secret, kc.AllowedModels)
		log.Info("key loaded", "id", k.ID)
	}
	timeout := cfg.Limits.RequestTimeoutDuration()
	cooldown := cfg.Limits.ProviderCooldownDuration()
	var providers []*provider.Provider
	for _, pc := range cfg.Providers {
		p := provider.New(
			pc.Name, pc.Type, pc.BaseURL, pc.Models, pc.Credentials,
			timeout, cooldown,
			pc.Concurrency, pc.RPM, pc.TPM, pc.TPH, pc.TPD,
		)
		p.UpdateHeaders(pc.Headers, pc.PassHeaders)
		providers = append(providers, p)
		log.Info("provider loaded",
			"name", pc.Name, "type", pc.Type, "base_url", pc.BaseURL, "credentials", len(pc.Credentials))
	}
	return provider.NewManager(providers, yamlRoutes(cfg))
}
