// Package provider 实现上游 Provider 抽象：
// OpenAI Compatible 适配器、多凭证管理、冷却与故障分类。
// 上游原始错误由 normalize 收敛，本包只负责"选谁、怎么调、失败怎么标记"。
// 限流额度（并发/RPM/TPM/TPH/TPD）属于 Provider：上游账号的配额限制。
package provider

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"harbor/internal/normalize"
)

// CredStatus 是凭证运行时状态。
type CredStatus int

const (
	CredActive CredStatus = iota
	CredCooling
	CredDisabled
)

// Credential 是上游凭证。
type Credential struct {
	ID            string
	Secret        string
	Status        CredStatus
	CooldownUntil time.Time
}

func (c *Credential) String() string {
	return c.StatusLabel()
}

// StatusLabel 返回状态文本。
func (c *Credential) StatusLabel() string {
	switch c.Status {
	case CredActive:
		return "active"
	case CredCooling:
		return fmt.Sprintf("cooling_until=%s", c.CooldownUntil.Format("15:04:05"))
	case CredDisabled:
		return "disabled"
	}
	return "unknown"
}

// Credentials 返回凭证快照（含状态）。
func (p *Provider) Credentials() []*Credential {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]*Credential, len(p.creds))
	for i, c := range p.creds {
		copy := *c
		out[i] = &copy
	}
	return out
}

// Limits 是 Provider 的限流额度快照。
type Limits struct {
	Concurrency int   `json:"concurrency"`
	RPM         int   `json:"rpm"`
	TPM         int64 `json:"tpm"`
	TPH         int64 `json:"tph"`
	TPD         int64 `json:"tpd"`
}

// ModelSpec 是 Provider 上一个上游模型的声明与展示元数据。
// Enabled/Visible 用指针：nil 表示默认 true，便于 API/YAML 省略字段。
type ModelSpec struct {
	ID           string   `json:"id"`
	Enabled      *bool    `json:"enabled,omitempty"`      // false = 不参与路由与列表（软删除）
	Visible      *bool    `json:"visible,omitempty"`      // false = 不单独对外暴露，但仍可被映射引用
	Context      int      `json:"context,omitempty"`      // 上下文窗口（token），0 = 未声明
	Capabilities []string `json:"capabilities,omitempty"` // chat / embedding / vision / reasoning / tools
	// ConcurrencyCost 是该上游模型每次请求占用的并发槽系数，默认 1.0。
	// 0.25 / 0.5 = 轻量模型共享并发槽；1.2–3 = 重型模型占多个槽。
	ConcurrencyCost float64 `json:"concurrency_cost,omitempty"`
}

// IsEnabled 返回模型是否参与路由。
func (m ModelSpec) IsEnabled() bool { return m.Enabled == nil || *m.Enabled }

// IsVisible 返回模型是否可单独出现在 /v1/models。
func (m ModelSpec) IsVisible() bool { return m.Visible == nil || *m.Visible }

// ModelCaps 是模型能力的可选取值（管理台按此渲染，写入时过滤未知值）。
var ModelCaps = []string{"chat", "embedding", "vision", "reasoning", "tools"}

var modelCapSet = func() map[string]bool {
	out := make(map[string]bool, len(ModelCaps))
	for _, c := range ModelCaps {
		out[c] = true
	}
	return out
}()

// NormalizeSpecs 去空、去重、裁剪能力取值（导出给 API 层复用）。
func NormalizeSpecs(specs []ModelSpec) []ModelSpec {
	out := make([]ModelSpec, 0, len(specs))
	seen := make(map[string]bool, len(specs))
	for _, s := range specs {
		s.ID = strings.TrimSpace(s.ID)
		if s.ID == "" || seen[s.ID] {
			continue
		}
		seen[s.ID] = true
		caps := make([]string, 0, len(s.Capabilities))
		capSeen := make(map[string]bool, len(s.Capabilities))
		for _, c := range s.Capabilities {
			c = strings.ToLower(strings.TrimSpace(c))
			if c == "" || !modelCapSet[c] || capSeen[c] {
				continue
			}
			capSeen[c] = true
			caps = append(caps, c)
		}
		s.Capabilities = caps
		if s.Context < 0 {
			s.Context = 0
		}
		if s.ConcurrencyCost <= 0 || math.IsNaN(s.ConcurrencyCost) || math.IsInf(s.ConcurrencyCost, 0) {
			s.ConcurrencyCost = 1
		}
		if s.ConcurrencyCost > 1000 {
			s.ConcurrencyCost = 1000
		}
		out = append(out, s)
	}
	return out
}

// SpecsFromIDs 把一组模型名转成默认声明（启用 + 可见 + chat 能力）。
func SpecsFromIDs(ids []string) []ModelSpec {
	specs := make([]ModelSpec, 0, len(ids))
	for _, id := range ids {
		if strings.TrimSpace(id) == "" {
			continue
		}
		specs = append(specs, ModelSpec{ID: id, Capabilities: []string{"chat"}})
	}
	return NormalizeSpecs(specs)
}

// Snapshot 是 Provider 配置与限额的一致快照。
type Snapshot struct {
	Name        string
	Type        string
	BaseURL     string
	Models      []string
	Specs       []ModelSpec
	Timeout     time.Duration
	Cooldown    time.Duration
	Concurrency int
	RPM         int
	TPM         int64
	TPH         int64
	TPD         int64
	PublicOnly  bool
}

func (p *Provider) Snapshot() Snapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()
	specs := make([]ModelSpec, len(p.Specs))
	for i, s := range p.Specs {
		specs[i] = s
		specs[i].Capabilities = append([]string(nil), s.Capabilities...)
	}
	return Snapshot{
		Name: p.Name, Type: p.Type, BaseURL: p.BaseURL,
		Models:  append([]string(nil), p.Models...),
		Specs:   specs,
		Timeout: p.Timeout, Cooldown: p.Cooldown,
		Concurrency: p.Concurrency, RPM: p.RPM,
		TPM: p.TPM, TPH: p.TPH, TPD: p.TPD,
		PublicOnly: p.PublicOnly,
	}
}

func (p *Provider) SupportsModel(model string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, candidate := range p.Models {
		if candidate == model {
			return true
		}
	}
	return false
}

// Provider 是单个上游.
type Provider struct {
	Name    string
	Type    string
	BaseURL string
	// Models 是「已启用」的上游模型名（由 Specs 派生），路由只看这个列表。
	Models []string
	// Specs 是完整模型声明（含禁用/不展示），持久化与展示用。
	Specs    []ModelSpec
	Timeout  time.Duration
	Cooldown time.Duration

	// 限流额度（属于上游账号的配额，非 Harbor Key）
	Concurrency int
	RPM         int
	TPM         int64
	TPH         int64
	TPD         int64
	PublicOnly  bool

	mu    sync.RWMutex
	creds []*Credential
	rr    int // round-robin 游标
}

var upstreamHTTPClient = &http.Client{
	Transport: http.DefaultTransport,
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 3 {
			return fmt.Errorf("upstream redirect limit exceeded")
		}
		if len(via) > 0 {
			previous := via[len(via)-1].URL
			if req.URL.Scheme != previous.Scheme || req.URL.Host != previous.Host {
				return fmt.Errorf("cross-origin upstream redirect blocked")
			}
		}
		return nil
	},
}

// JoinURL 拼接上游 BaseURL 与请求路径。
// OpenAI 兼容约定：BaseURL 通常已含 /v1（https://api.openai.com/v1），
// Harbor 对外路径是 /v1/chat/completions。直接拼接会打成 /v1/v1/...，
// 上游（如 airouter / LiteLLM）返回 HTML 404。本函数去掉重复前缀，
// 同时兼容 BaseURL 不含 /v1 的写法。
func JoinURL(base, path string) string {
	base = strings.TrimRight(strings.TrimSpace(base), "/")
	path = strings.TrimSpace(path)
	if path == "" {
		return base
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	for {
		rest := path[1:]
		slash := strings.IndexByte(rest, '/')
		var seg string
		if slash < 0 {
			seg = path
		} else {
			seg = path[:slash+1]
		}
		if seg == "/" {
			break
		}
		if strings.HasSuffix(base, seg) {
			path = path[len(seg):]
			if path == "" {
				return base
			}
			if !strings.HasPrefix(path, "/") {
				path = "/" + path
			}
			continue
		}
		break
	}
	return base + path
}

// New 构造 Provider（base 去尾部斜杠）。
func New(name, typ, baseURL string, models, secrets []string, timeout, cooldown time.Duration,
	concurrency int, rpm int, tpm, tph, tpd int64) *Provider {
	p := &Provider{
		Name:        name,
		Type:        typ,
		BaseURL:     strings.TrimRight(baseURL, "/"),
		Timeout:     timeout,
		Cooldown:    cooldown,
		Concurrency: concurrency,
		RPM:         rpm,
		TPM:         tpm,
		TPH:         tph,
		TPD:         tpd,
	}
	for _, s := range secrets {
		p.creds = append(p.creds, &Credential{ID: credID(), Secret: s, Status: CredActive})
	}
	p.SetModelSpecs(SpecsFromIDs(models))
	return p
}

func credID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return "crd_" + hex.EncodeToString(b)
}

// Limits 返回限流额度快照。
func (p *Provider) Limits() Limits {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return Limits{
		Concurrency: p.Concurrency,
		RPM:         p.RPM,
		TPM:         p.TPM,
		TPH:         p.TPH,
		TPD:         p.TPD,
	}
}

// UpdateLimits 更新限流额度（管理 API 用；对后续请求即时生效）。
func (p *Provider) UpdateLimits(concurrency int, rpm int, tpm, tph, tpd int64) {
	p.mu.Lock()
	p.Concurrency = concurrency
	p.RPM = rpm
	p.TPM = tpm
	p.TPH = tph
	p.TPD = tpd
	p.mu.Unlock()
}

func (p *Provider) UpdateType(typ string) {
	if typ == "" {
		return
	}
	p.mu.Lock()
	p.Type = typ
	p.mu.Unlock()
}

func (p *Provider) SetPublicOnly(publicOnly bool) {
	p.mu.Lock()
	p.PublicOnly = publicOnly
	p.mu.Unlock()
}

func (p *Provider) UpdateTimeoutCooldown(timeout, cooldown time.Duration) {
	p.mu.Lock()
	p.Timeout = timeout
	p.Cooldown = cooldown
	p.mu.Unlock()
}

// UpdateBaseURL 更新上游地址（空值忽略）。
func (p *Provider) UpdateBaseURL(url string) {
	if url != "" {
		p.mu.Lock()
		p.BaseURL = strings.TrimRight(url, "/")
		p.mu.Unlock()
	}
}

// UpdateModels 更新模型列表（保留已有模型的启用/可见/上下文/能力元数据）。
func (p *Provider) UpdateModels(models []string) {
	p.mu.RLock()
	existing := make(map[string]ModelSpec, len(p.Specs))
	for _, s := range p.Specs {
		existing[s.ID] = s
	}
	p.mu.RUnlock()
	specs := make([]ModelSpec, 0, len(models))
	for _, id := range models {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if s, ok := existing[id]; ok {
			specs = append(specs, s)
			continue
		}
		specs = append(specs, ModelSpec{ID: id, Capabilities: []string{"chat"}})
	}
	p.SetModelSpecs(specs)
}

// ModelSpec 按模型名取声明（存在性由第二个返回值表示）。
func (p *Provider) ModelSpec(id string) (ModelSpec, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, s := range p.Specs {
		if s.ID == id {
			return s, true
		}
	}
	return ModelSpec{}, false
}

// ModelSpecs 返回模型声明快照（含禁用项）。
func (p *Provider) ModelSpecs() []ModelSpec {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]ModelSpec, len(p.Specs))
	for i, s := range p.Specs {
		out[i] = s
		out[i].Capabilities = append([]string(nil), s.Capabilities...)
	}
	return out
}

// SetModelSpecs 覆盖模型声明；Models 只保留「已启用」的模型（路由依据）。
func (p *Provider) SetModelSpecs(specs []ModelSpec) {
	specs = NormalizeSpecs(specs)
	models := make([]string, 0, len(specs))
	for _, s := range specs {
		if s.IsEnabled() {
			models = append(models, s.ID)
		}
	}
	p.mu.Lock()
	p.Specs = specs
	p.Models = models
	p.mu.Unlock()
}

// AddCredential 添加一个凭证（管理 API 用）。
func (p *Provider) AddCredential(secret string) *Credential {
	c := &Credential{ID: credID(), Secret: secret, Status: CredActive}
	p.mu.Lock()
	p.creds = append(p.creds, c)
	p.mu.Unlock()
	return c
}

// RestoreCredential 恢复持久化凭证；同一 secret 已由配置加载时只更新其元数据。
func (p *Provider) RestoreCredential(id, secret string, status CredStatus, cooldownUntil time.Time) *Credential {
	if secret == "" {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.creds {
		if c.Secret == secret {
			if id != "" {
				c.ID = id
			}
			c.Status = status
			c.CooldownUntil = cooldownUntil
			return c
		}
	}
	if id == "" {
		id = credID()
	}
	c := &Credential{ID: id, Secret: secret, Status: status, CooldownUntil: cooldownUntil}
	p.creds = append(p.creds, c)
	return c
}

// RemoveCredential 按 ID 移除凭证（管理 API 用）。
func (p *Provider) RemoveCredential(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, c := range p.creds {
		if c.ID == id {
			p.creds = append(p.creds[:i], p.creds[i+1:]...)
			return true
		}
	}
	return false
}

// Available 是否有至少一个可用凭证（active 且未冷却）。
func (p *Provider) Available() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	for _, c := range p.creds {
		if c.Status == CredActive || (c.Status == CredCooling && c.CooldownUntil.Before(now)) {
			return true
		}
	}
	return false
}

// NextRecover 所有凭证冷却时，最早恢复时间；有可用则 now。
func (p *Provider) NextRecover() time.Time {
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	var earliest time.Time
	for _, c := range p.creds {
		if c.Status == CredActive {
			return now
		}
		if c.Status == CredCooling && (earliest.IsZero() || c.CooldownUntil.Before(earliest)) {
			earliest = c.CooldownUntil
		}
	}
	if earliest.IsZero() {
		return now
	}
	return earliest
}

// pick 选择一个可用凭证（round-robin）。
func (p *Provider) pick() (*Credential, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	n := len(p.creds)
	for i := 0; i < n; i++ {
		idx := (p.rr + i) % n
		c := p.creds[idx]
		if c.Status == CredActive || (c.Status == CredCooling && c.CooldownUntil.Before(now)) {
			if c.Status == CredCooling {
				c.Status = CredActive
				c.CooldownUntil = time.Time{}
			}
			p.rr = (idx + 1) % n
			return c, true
		}
	}
	return nil, false
}

// markFail 根据错误类别更新凭证状态。
// 429 / 5xx / timeout → 冷却；401 / 403 → 禁用。
func (p *Provider) markFail(cred *Credential, cls normalize.Class, retryAt ...time.Time) time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	// 在途请求可能晚于禁用操作失败，不能把已禁用凭证改回冷却状态。
	if cred.Status == CredDisabled {
		return time.Time{}
	}
	switch cls {
	case normalize.ClassRateLimit, normalize.ClassUpstream5xx, normalize.ClassTimeout:
		until := time.Now().Add(p.Cooldown)
		for _, candidate := range retryAt {
			if candidate.After(until) {
				until = candidate
			}
		}
		cred.Status = CredCooling
		if until.After(cred.CooldownUntil) {
			cred.CooldownUntil = until
		}
		return cred.CooldownUntil
	case normalize.ClassUpstreamKey:
		cred.Status = CredDisabled
	default:
		// 保守：不标记
	}
	return time.Time{}
}

// ModelRoute 是一条模型映射：统一模型名 → (上游 Provider, 上游模型名)。
// 用于把不同渠道同名不同称的模型统一为一个入口模型名。
type ModelRoute struct {
	Provider      string `json:"provider"`       // 上游 Provider 名称
	UpstreamModel string `json:"upstream_model"` // 空 = 与统一模型名相同（自动路由场景）
}

// Manager 持有所有 Provider 与模型映射（支持运行时增删改）。
type Manager struct {
	mu        sync.RWMutex
	providers map[string]*Provider
	mappings  map[string][]ModelRoute // unified model -> routes（顺序即优先级）
	timeout   time.Duration
	cooldown  time.Duration
}

// NewManager 从运行时 Provider 列表构造。
func NewManager(providers []*Provider, mappings map[string][]ModelRoute) *Manager {
	m := &Manager{providers: map[string]*Provider{}, mappings: map[string][]ModelRoute{}}
	for _, p := range providers {
		m.providers[p.Name] = p
	}
	for k, v := range mappings {
		m.mappings[k] = v
	}
	return m
}

// Get 按名字获取 Provider。
func (m *Manager) Get(name string) (*Provider, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.providers[name]
	return p, ok
}

// GetProvider 按名字获取 Provider（Get 的别名）。
func (m *Manager) GetProvider(name string) (*Provider, bool) { return m.Get(name) }

// Providers 返回全部 Provider。
func (m *Manager) Providers() []*Provider {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Provider, 0, len(m.providers))
	for _, p := range m.providers {
		out = append(out, p)
	}
	return out
}

// AddProvider 注册一个新 Provider（管理 API 用）。
func (m *Manager) AddProvider(p *Provider) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.providers[p.Name] = p
}

// RemoveProvider 移除 Provider 及其关联映射（管理 API 用）。
func (m *Manager) RemoveProvider(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.providers[name]; !ok {
		return false
	}
	delete(m.providers, name)
	for model, routes := range m.mappings {
		var kept []ModelRoute
		for _, r := range routes {
			if r.Provider != name {
				kept = append(kept, r)
			}
		}
		if len(kept) == 0 {
			delete(m.mappings, model)
		} else {
			m.mappings[model] = kept
		}
	}
	return true
}

// SetRoute 设置或更新模型映射（管理 API 用）。provider 引用必须全部存在。
func (m *Manager) SetRoute(model string, routes []ModelRoute) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range routes {
		if _, ok := m.providers[r.Provider]; !ok {
			return false
		}
	}
	m.mappings[model] = routes
	return true
}

// RemoveRoute 删除模型映射（管理 API 用）。
func (m *Manager) RemoveRoute(model string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.mappings[model]; !ok {
		return false
	}
	delete(m.mappings, model)
	return true
}

// UpstreamModelFor 返回统一模型在某 Provider 上的上游模型名。
// 显式映射优先；没有显式映射时按自动路由处理（上游模型名 = 统一模型名）。
func (m *Manager) UpstreamModelFor(model, providerName string) (string, bool) {
	m.mu.RLock()
	routes, explicit := m.mappings[model]
	m.mu.RUnlock()
	if explicit {
		for _, r := range routes {
			if r.Provider != providerName {
				continue
			}
			upstream := r.UpstreamModel
			if upstream == "" {
				upstream = model
			}
			return upstream, true
		}
		return "", false
	}
	p, ok := m.Get(providerName)
	if !ok || !p.SupportsModel(model) {
		return "", false
	}
	return model, true
}

// ModelConcurrencyCost 返回统一模型在指定 Provider 上的并发占用系数。
// 优先读 Provider 模型声明里的 concurrency_cost；未声明返回 0（由调用方兜底）。
func (m *Manager) ModelConcurrencyCost(model, providerName string) float64 {
	upstream, ok := m.UpstreamModelFor(model, providerName)
	if !ok {
		return 0
	}
	p, ok := m.Get(providerName)
	if !ok {
		return 0
	}
	if spec, declared := p.ModelSpec(upstream); declared && spec.ConcurrencyCost > 0 {
		return spec.ConcurrencyCost
	}
	return 0
}

// ModelProviders 返回某模型按优先级排列的 Provider。
// 先查显式映射；无映射时自动路由（检查各 Provider 的 Models 列表）。
func (m *Manager) ModelProviders(model string) []*Provider {
	m.mu.RLock()
	routes, ok := m.mappings[model]
	m.mu.RUnlock()
	if ok {
		out := make([]*Provider, 0, len(routes))
		for _, r := range routes {
			p, ok := m.Get(r.Provider)
			if !ok {
				continue
			}
			// 显式映射同样尊重「禁用」：声明存在且被禁用时不再参与路由。
			upstream := r.UpstreamModel
			if upstream == "" {
				upstream = model
			}
			if spec, declared := p.ModelSpec(upstream); declared && !spec.IsEnabled() {
				continue
			}
			out = append(out, p)
		}
		if len(out) > 0 {
			return out
		}
	}
	// 自动路由：查找 Models 列表包含该模型的 Provider
	m.mu.RLock()
	providers := make([]*Provider, 0, len(m.providers))
	for _, p := range m.providers {
		providers = append(providers, p)
	}
	m.mu.RUnlock()
	var out []*Provider
	for _, p := range providers {
		if p.SupportsModel(model) {
			out = append(out, p)
		}
	}
	return out
}

// UpstreamModel 返回某统一模型在指定 Provider 上的上游模型名。
// 无显式映射时返回统一模型名本身（自动路由场景）。
func (m *Manager) UpstreamModel(model, providerName string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if routes, ok := m.mappings[model]; ok {
		for _, r := range routes {
			if r.Provider == providerName && r.UpstreamModel != "" {
				return r.UpstreamModel
			}
		}
	}
	return model
}

// PublicModels 返回对外可见的模型名（/v1/models 与公开状态用）。
// 客户端只应看到「统一模型名」，规则：
//   - 显式路由的 key（统一名）全部对外；
//   - Provider 的 models 列表里，被任一映射当作 upstream_model 使用的名字不再单独暴露，
//     否则客户端会看到映射前的上游模型名（例如 deepseek-v4.1-flash:dev）；
//   - 既不是路由 key、也没有被映射走的名字，按同名直通对外。
func (m *Manager) PublicModels() []string {
	m.mu.RLock()
	mappings := make(map[string][]ModelRoute, len(m.mappings))
	for model, routes := range m.mappings {
		mappings[model] = append([]ModelRoute(nil), routes...)
	}
	providers := make([]*Provider, 0, len(m.providers))
	for _, p := range m.providers {
		providers = append(providers, p)
	}
	m.mu.RUnlock()

	unified := make(map[string]bool, len(mappings))
	upstreamOnly := map[string]bool{}
	for model, routes := range mappings {
		unified[model] = true
		for _, r := range routes {
			// upstream_model 为空表示同名直通，不算“被映射走”。
			if r.UpstreamModel != "" && r.UpstreamModel != model {
				upstreamOnly[r.UpstreamModel] = true
			}
		}
	}

	visible := make(map[string]bool, len(unified))
	for model := range unified {
		visible[model] = true
	}
	for _, p := range providers {
		for _, spec := range p.ModelSpecs() {
			if !spec.IsEnabled() || !spec.IsVisible() {
				continue
			}
			if unified[spec.ID] || upstreamOnly[spec.ID] {
				continue
			}
			visible[spec.ID] = true
		}
	}

	out := make([]string, 0, len(visible))
	for name := range visible {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// Routes 返回模型映射表快照。
// ModelSpec 返回统一模型名对应的声明规格（用于 /v1/models 元数据）。
func (m *Manager) ModelSpec(model string) (ModelSpec, bool) {
	m.mu.RLock()
	providers := make([]*Provider, 0, len(m.providers))
	for _, p := range m.providers {
		providers = append(providers, p)
	}
	m.mu.RUnlock()
	for _, p := range providers {
		if spec, ok := p.ModelSpec(model); ok {
			return spec, true
		}
	}
	for _, p := range providers {
		up := m.UpstreamModel(model, p.Name)
		if up == "" || up == model {
			continue
		}
		if spec, ok := p.ModelSpec(up); ok {
			return spec, true
		}
	}
	return ModelSpec{}, false
}
func (m *Manager) Routes() map[string][]ModelRoute {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string][]ModelRoute, len(m.mappings))
	for k, v := range m.mappings {
		out[k] = append([]ModelRoute(nil), v...)
	}
	return out
}

// Response 是统一的上游响应（内存版 / 流式版）。
type Response struct {
	StatusCode  int
	Header      http.Header
	Body        []byte // 非流式响应体
	Stream      bool
	StreamBody  io.ReadCloser // 流式响应体（调用方负责 Close）
	Usage       Usage
	UsageKnown  bool   // 上游明确返回 usage 时为 true
	Model       string // 上游响应体回显的模型名（非流式）
	HeaderMs    int64  // 发出上游请求到收到响应头的耗时（毫秒）
	ContentType string // 上游响应头 Content-Type
}

// Usage 请求的 token 用量（从响应解析）。
type Usage struct {
	PromptTokens      int
	CompletionTokens  int
	TotalTokens       int
	CachedTokens      int // 命中前缀缓存的输入 token（prompt_tokens_details.cached_tokens）
	CachedWriteTokens int // 写入缓存的 token（cache_creation_input_tokens / cache_write_tokens）
}

// UsageReader 暴露流式响应中解析到的最终 usage。
// 流读取结束后调用 StreamUsage；未返回 usage 时 known=false。
type UsageReader interface {
	StreamUsage() (Usage, bool)
}

type streamUsageBody struct {
	src     io.ReadCloser
	mu      sync.Mutex
	pending []byte
	usage   Usage
	known   bool
	model   string
}

func newStreamUsageBody(src io.ReadCloser) *streamUsageBody {
	return &streamUsageBody{src: src}
}

func (b *streamUsageBody) Read(p []byte) (int, error) {
	n, err := b.src.Read(p)
	if n > 0 {
		b.consume(p[:n], err == io.EOF)
	} else if err == io.EOF {
		b.consume(nil, true)
	}
	return n, err
}

func (b *streamUsageBody) Close() error { return b.src.Close() }

func (b *streamUsageBody) StreamUsage() (Usage, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.usage, b.known
}

// maxPendingLine 限制单条未换行 SSE 数据的累积长度。
// usage 单行 JSON 远小于此值；超过说明上游已不按 SSE 语义发送，
// 丢弃该行（透传字节不受影响），防止内存无限增长。
const maxPendingLine = 4 << 20

func (b *streamUsageBody) consume(chunk []byte, eof bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for len(chunk) > 0 {
		i := bytes.IndexByte(chunk, '\n')
		if i < 0 {
			// 当前块无换行：与 pending 拼接后若超限，则这一行
			// 是超长行，放弃（有界缓冲，不无限增长）。
			if len(b.pending)+len(chunk) > maxPendingLine {
				b.pending = b.pending[:0]
				return
			}
			b.pending = append(b.pending, chunk...)
			break
		}
		// 找到换行：完整行 = pending + chunk[:i]。超长行丢弃。
		line := append(b.pending, chunk[:i]...)
		if len(line) <= maxPendingLine {
			b.parseLineLocked(line)
		}
		b.pending = b.pending[:0]
		chunk = chunk[i+1:]
	}
	if eof {
		if len(b.pending) > 0 && len(b.pending) <= maxPendingLine {
			b.parseLineLocked(b.pending)
		}
		b.pending = b.pending[:0]
	}
}

func (b *streamUsageBody) parseLineLocked(line []byte) {
	line = bytes.TrimSpace(line)
	if !bytes.HasPrefix(line, []byte("data:")) {
		return
	}
	data := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
	if len(data) == 0 || bytes.Equal(data, []byte("[DONE]")) {
		return
	}
	if b.model == "" {
		var meta struct {
			Model string `json:"model"`
		}
		if json.Unmarshal(data, &meta) == nil && meta.Model != "" {
			b.model = meta.Model
		}
	}
	if usage, ok := parseUsage(data); ok {
		b.usage = usage
		b.known = true
	}
}

// StreamModel 返回流式响应中出现过的模型名（首个带 model 的 data 行）。
func (b *streamUsageBody) StreamModel() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.model
}

// Request 是上游调用请求（透传语义）。
type Request struct {
	Path    string            // Harbor 对外路径，如 /v1/chat/completions；与 BaseURL 拼接时去重 /v1
	Body    []byte            // 原始请求体
	Headers map[string]string // 需要透传给上游的头
	Stream  bool
	Timeout time.Duration // 0 = 使用 provider 默认
	// TTFBTimeout 仅流式生效：等待上游响应头的上限（0 = 使用 provider 默认超时）。
	// 收到响应头后立即解除，不再限制整条流的生命周期。
	TTFBTimeout time.Duration
}

// parseRetryAfter 解析 delta-seconds / HTTP-date；无效、过期或超出 Duration 范围则忽略。
func parseRetryAfter(value string, now time.Time) time.Time {
	value = strings.TrimSpace(value)
	const maxDelay time.Duration = 1<<63 - 1
	if seconds, err := strconv.ParseUint(value, 10, 64); err == nil {
		if seconds <= uint64(maxDelay/time.Second) {
			return now.Add(time.Duration(seconds) * time.Second)
		}
		return time.Time{}
	}
	if date, err := http.ParseTime(value); err == nil && date.After(now) && !date.After(now.Add(maxDelay)) {
		return date
	}
	return time.Time{}
}

// Execute 执行一次上游调用。错误均为 normalize.Error。
func (p *Provider) Execute(ctx context.Context, req *Request) (*Response, error) {
	cred, ok := p.pick()
	if !ok {
		return nil, normalize.Upstream(p.Name, 503, "no available credential")
	}

	snapshot := p.Snapshot()
	url := JoinURL(snapshot.BaseURL, req.Path)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(req.Body))
	if err != nil {
		return nil, normalize.New(normalize.ErrInternal, fmt.Sprintf("build upstream request: %v", err))
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+cred.Secret)
	httpReq.Header.Set("Accept", "application/json")
	if req.Stream {
		httpReq.Header.Set("Accept", "text/event-stream")
	}
	for k, v := range req.Headers {
		if k == "Authorization" || k == "Content-Type" || k == "Accept" {
			continue // 不允许客户端覆盖上游凭证相关头
		}
		httpReq.Header.Set(k, v)
	}
	if req.Stream {
		// SSE 必须直接传递增量：默认 gzip 协商会让未同步刷新的压缩层
		// 缓存数据到流结束。放在自定义头之后，保证流式约束不会被覆盖。
		httpReq.Header.Set("Accept-Encoding", "identity")
		httpReq.Header.Set("Cache-Control", "no-cache, no-transform")
	}

	timeout := req.Timeout
	if timeout == 0 {
		timeout = snapshot.Timeout
	}
	// 非流式请求在 Execute 返回前读完响应，provider 级总超时无副作用。
	// 流式请求不能套整段 WithTimeout：Execute 返回后取消 ctx 会关闭底层连接，
	// 导致 SSE body 只读到首块就 EOF。流式超时拆成两段：
	//   1) TTFB：只覆盖「发出请求 → 收到响应头」，收到头即 Stop；
	//   2) 空闲读超时：由 server.copyStream 按数据块重置。
	if timeout > 0 && !req.Stream {
		cctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		httpReq = httpReq.WithContext(cctx)
	}

	var streamCancel context.CancelFunc
	var ttfbTimer *time.Timer
	var ttfbFired atomic.Bool
	if req.Stream {
		streamCtx, cancel := context.WithCancel(ctx)
		streamCancel = cancel
		httpReq = httpReq.WithContext(streamCtx)
		ttfb := req.TTFBTimeout
		if ttfb <= 0 {
			ttfb = snapshot.Timeout
		}
		if ttfb > 0 {
			ttfbTimer = time.AfterFunc(ttfb, func() {
				ttfbFired.Store(true)
				cancel()
			})
		}
	}

	client := upstreamHTTPClient
	if snapshot.PublicOnly {
		client = publicOnlyHTTPClient
	}
	startedAt := time.Now()
	resp, err := client.Do(httpReq)
	if ttfbTimer != nil {
		ttfbTimer.Stop()
	}
	headerMs := time.Since(startedAt).Milliseconds()
	if err != nil {
		if streamCancel != nil {
			streamCancel()
		}
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		// 客户端/请求级取消优先判定：这种取消不是上游故障，不能冷却凭证。
		if ctx.Err() == context.DeadlineExceeded {
			return nil, normalize.New(normalize.ErrTimeout, "request deadline exceeded")
		}
		if ctx.Err() != nil {
			he := normalize.New(normalize.ErrInternal, "client cancelled")
			he.Class = normalize.ClassUnknown
			he.Retryable = false
			return nil, he
		}
		// TTFB 到点：明确归类为超时。
		if ttfbFired.Load() {
			he := normalize.New(normalize.ErrTimeout, "upstream response header timeout")
			he.Class = normalize.ClassTimeout
			he.Retryable = true
			he.Provider = p.Name
			p.markFail(cred, he.Class)
			return nil, he
		}
		he := normalize.Upstream(p.Name, 0, err.Error())
		he.Class = normalize.ClassTimeout
		he.Retryable = true
		p.markFail(cred, he.Class)
		return nil, he
	}

	contentType := resp.Header.Get("Content-Type")
	// 上游错误响应：读取 body 用于分类，然后标记凭证。
	// 注意：不能 defer resp.Body.Close()——流式路径要把 body 所有权
	// 移交给调用方（server.awaitStream 负责 Close + 释放并发槽），
	// 提前关闭会截断 SSE 流。错误路径与非流式路径在此显式关闭。
	if resp.StatusCode >= 400 {
		if streamCancel != nil {
			streamCancel()
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		he := normalize.Upstream(p.Name, resp.StatusCode, string(body))
		he.Reason = fmt.Sprintf("%s url=%s", he.Reason, url)
		// 标记与 Retryable 解耦：429/5xx/timeout → 冷却；401/403 →
		// 凭证必须禁用（即使该错误本身不可重试，禁用后下一次尝试才能
		// 绕过坏凭证选到备用出口）。markFail 内部对未知类保守不标记。
		he.RetryAt = p.markFail(cred, he.Class, parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()))
		return nil, he
	}

	// 流式请求必须确认上游真的返回 SSE。否则普通 JSON（上游不支持 stream
	// 或鉴权失败仍回 200）会被当作 SSE 透传，客户端解析直接失败。
	if req.Stream && !strings.Contains(strings.ToLower(contentType), "text/event-stream") {
		resp.Body.Close()
		if streamCancel != nil {
			streamCancel()
		}
		he := normalize.New(normalize.ErrUpstream,
			fmt.Sprintf("upstream did not return text/event-stream (status=%d content_type=%q)", resp.StatusCode, contentType))
		he.Class = normalize.ClassUnknown
		he.Retryable = false
		he.Provider = p.Name
		he.UpstreamHTTP = resp.StatusCode
		// 协议不匹配不是凭证故障：不冷却上游凭证，只让本次请求明确失败。
		return nil, he
	}

	if req.Stream {
		// 流式路径：body 所有权移交调用方（由调用方 Close），此处不关闭。
		// 包装 body 只旁路解析 SSE usage，不修改透传字节；Close 时释放 TTFB context。
		var body io.ReadCloser = resp.Body
		if streamCancel != nil {
			body = &cancelOnCloseReadCloser{ReadCloser: body, cancel: streamCancel}
		}
		return &Response{
			StatusCode:  resp.StatusCode,
			Header:      resp.Header,
			Stream:      true,
			StreamBody:  newStreamUsageBody(body),
			HeaderMs:    headerMs,
			ContentType: contentType,
		}, nil
	}

	// 读取 max+1 字节：若实际上行超过 maxResponseBody，超过的部分
	// 会使 len(body) > maxResponseBody，从而识别为超限错误而非正常 EOF。
	const maxResponseBody = 64 << 20
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody+1))
	resp.Body.Close()
	if err != nil {
		he := normalize.Upstream(p.Name, 0, fmt.Sprintf("read response: %v", err))
		he.Class = normalize.ClassTimeout
		he.Retryable = true
		p.markFail(cred, he.Class)
		return nil, he
	}
	if len(body) > maxResponseBody {
		he := normalize.Upstream(p.Name, 0, "upstream response body exceeds limit")
		he.Class = normalize.ClassUnknown
		he.Retryable = false
		p.markFail(cred, he.Class)
		return nil, he
	}
	out := &Response{StatusCode: resp.StatusCode, Header: resp.Header, Body: body, HeaderMs: headerMs, ContentType: contentType}
	out.Usage, out.UsageKnown = parseUsage(body)
	out.Model = parseModelName(body)
	return out, nil
}

// cancelOnCloseReadCloser 在流式响应体关闭时释放 TTFB 派生 context，
// 确保上游连接不会因为 context 永远不取消而滞留。
type cancelOnCloseReadCloser struct {
	io.ReadCloser
	once   sync.Once
	cancel context.CancelFunc
}

func (c *cancelOnCloseReadCloser) Close() error {
	err := c.ReadCloser.Close()
	c.once.Do(c.cancel)
	return err
}

// parseModelName 读取响应体里的 model 字段（上游实际使用的模型）。
func parseModelName(body []byte) string {
	var raw struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return ""
	}
	return raw.Model
}

// parseUsage 从 OpenAI 兼容响应中解析 usage 字段。
// 返回 known 用来区分“usage 明确有效”和“响应没有可验证 usage”。
func parseUsage(body []byte) (Usage, bool) {
	var raw struct {
		Usage *struct {
			PromptTokens        *int `json:"prompt_tokens"`
			CompletionTokens    *int `json:"completion_tokens"`
			TotalTokens         *int `json:"total_tokens"`
			InputTokens         *int `json:"input_tokens"`
			OutputTokens        *int `json:"output_tokens"`
			CachedTokens        *int `json:"cached_tokens"`
			CacheWriteTokens    *int `json:"cache_write_tokens"`
			CacheCreationTokens *int `json:"cache_creation_input_tokens"`
			PromptTokensDetails *struct {
				CachedTokens     *int `json:"cached_tokens"`
				CacheWriteTokens *int `json:"cache_write_tokens"`
			} `json:"prompt_tokens_details"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &raw); err != nil || raw.Usage == nil {
		return Usage{}, false
	}
	u := raw.Usage
	prompt := u.PromptTokens
	if prompt == nil {
		prompt = u.InputTokens
	}
	completion := u.CompletionTokens
	if completion == nil {
		completion = u.OutputTokens
	}
	for _, value := range []*int{prompt, completion, u.TotalTokens} {
		if value != nil && *value < 0 {
			return Usage{}, false
		}
	}
	if prompt == nil && completion == nil && u.TotalTokens == nil {
		return Usage{}, false
	}

	promptValue, completionValue := 0, 0
	if prompt != nil {
		promptValue = *prompt
	}
	if completion != nil {
		completionValue = *completion
	}
	totalValue := 0
	if u.TotalTokens != nil {
		totalValue = *u.TotalTokens
	}
	// If total is absent, require both components. If components are absent,
	// total alone is still useful for quota settlement.
	if u.TotalTokens == nil {
		if prompt == nil || completion == nil {
			return Usage{}, false
		}
		if promptValue > int(^uint(0)>>1)-completionValue {
			return Usage{}, false
		}
		totalValue = promptValue + completionValue
	}
	if totalValue < promptValue+completionValue {
		totalValue = promptValue + completionValue
	}
	cached := u.CachedTokens
	if cached == nil && u.PromptTokensDetails != nil {
		cached = u.PromptTokensDetails.CachedTokens
	}
	cachedValue := 0
	if cached != nil && *cached > 0 {
		cachedValue = *cached
	}
	write := u.CacheCreationTokens
	if write == nil {
		write = u.CacheWriteTokens
	}
	if write == nil && u.PromptTokensDetails != nil {
		write = u.PromptTokensDetails.CacheWriteTokens
	}
	writeValue := 0
	if write != nil && *write > 0 {
		writeValue = *write
	}
	return Usage{PromptTokens: promptValue, CompletionTokens: completionValue, TotalTokens: totalValue, CachedTokens: cachedValue, CachedWriteTokens: writeValue}, true
}
