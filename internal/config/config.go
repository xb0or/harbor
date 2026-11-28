package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 是 Harbor 的根配置，对应 harbor.yaml。
type Config struct {
	Server    ServerConfig      `yaml:"server"`
	Queue     QueueConfig       `yaml:"queue"`
	Keepalive KeepaliveConfig   `yaml:"keepalive"`
	Providers []ProviderConfig  `yaml:"providers"`
	Routes    map[string]Route  `yaml:"routes"`
	Keys      []KeyConfig       `yaml:"keys"`
	Limits    RetryLimitsConfig `yaml:"limits"`
	Admin     AdminConfig       `yaml:"admin"`
	Log       LogConfig         `yaml:"log"`
	Storage   StorageConfig     `yaml:"storage"`
}

// KeepaliveConfig 是长连接心跳保活配置（SSE / 流式响应）。
// 当连接上长时间没有数据流动时，网关主动向下游写一个空事件，
// 避免中间代理或客户端空闲超时把长连接掐断。
type KeepaliveConfig struct {
	// Enabled 总开关，默认 false：只有显式开启才会在空闲时补心跳，
	// 避免首帧就插入 `: keepalive` 注释帧影响严格 SSE 客户端。
	Enabled *bool `yaml:"enabled"`
	// Interval 空闲多久发一次心跳，默认 15s。
	Interval string `yaml:"interval"`
	// Style 发送形式：comment（SSE 注释行，默认，OpenAI SDK 会忽略）
	// 或 event（data: 行，带 type=keepalive 的 JSON）。
	Style string `yaml:"style"`
	// Comment comment 模式的注释文本，默认 "keepalive"。
	Comment string `yaml:"comment"`
}

// IsEnabled 返回心跳是否开启（必须显式 enabled: true）。
func (k KeepaliveConfig) IsEnabled() bool { return k.Enabled != nil && *k.Enabled }

// IntervalDuration 返回空闲阈值。
func (k KeepaliveConfig) IntervalDuration() time.Duration {
	d := parseDur(k.Interval, 15*time.Second)
	if d <= 0 {
		d = 15 * time.Second
	}
	return d
}

// StyleValue 归一化发送形式。
func (k KeepaliveConfig) StyleValue() string {
	switch strings.ToLower(strings.TrimSpace(k.Style)) {
	case "event", "data":
		return "event"
	default:
		return "comment"
	}
}

// Frame 返回一次心跳要写入连接的原始字节。
func (k KeepaliveConfig) Frame() string {
	if k.StyleValue() == "event" {
		return "data: {\"type\":\"keepalive\"}\n\n"
	}
	comment := strings.TrimSpace(k.Comment)
	if comment == "" {
		comment = "keepalive"
	}
	comment = strings.NewReplacer("\r", " ", "\n", " ").Replace(comment)
	return ": " + comment + "\n\n"
}

// StorageConfig 是持久化配置（P2）。
// DBPath 为空 = 纯内存模式（P0 行为，零迁移成本）；
// 配置后 Harbor 将 SQLite 作为审计/镜像存储：请求记录、Key 元数据、
// 上游凭证（AES-GCM 加密）落库，内存组件仍是运行时权威。
type StorageConfig struct {
	// DBPath SQLite 文件路径，如 data/harbor.db；空 = 不启用持久化
	DBPath string `yaml:"db_path"`
	// KeyPath 上游凭证加密主密钥文件；空 = 默认 harbor.key（与 DBPath 同级工作目录）
	// 文件不存在时自动生成 32 字节随机密钥（0600）。丢失后无法解密已存凭证。
	KeyPath string `yaml:"key_path"`
}

type ServerConfig struct {
	Listen      string `yaml:"listen"`
	MaxBodySize string `yaml:"max_body_size"`
	// MaxHeaderBytes 可选，默认 1MB
	MaxHeaderBytes int `yaml:"max_header_bytes"`
	// ReadTimeout 限制读取完整请求（包括 Body）的时长，默认 30s。
	ReadTimeout string `yaml:"read_timeout"`
	// IdleTimeout 限制 keep-alive 空闲连接，默认 120s。
	IdleTimeout string `yaml:"idle_timeout"`
	// MaxBodyBytesInFlight 是所有请求 Body/排队 Body 的内存预算，默认 256MB。
	MaxBodyBytesInFlight string `yaml:"max_body_bytes_in_flight"`
}

type QueueConfig struct {
	MaxWait  string `yaml:"max_wait"`
	MaxDepth int    `yaml:"max_depth"`
	// Early429After 排队超过该时长仍未调度就提前返回 429（空/0 = 关闭）。
	Early429After string `yaml:"early_429_after"`
	Keepalive     string `yaml:"keepalive"`
	// ExposeQueuePosition 是否在响应头暴露 X-Queue-Position
	ExposeQueuePosition bool `yaml:"expose_queue_position"`
}

// ProviderConfig 描述一个上游。
type ProviderConfig struct {
	Name        string   `yaml:"name"`
	Type        string   `yaml:"type"` // openai-compatible | custom-http
	BaseURL     string   `yaml:"base_url"`
	Credentials []string `yaml:"credentials"` // 支持 ${ENV_VAR} 展开
	Models      []string `yaml:"models"`      // 可选，该上游支持的模型
	// Timeout 单次上游请求超时，默认 120s
	Timeout string `yaml:"timeout"`
	// Cooldown 失败后冷却时长，默认取 limits.provider_cooldown
	Cooldown string `yaml:"cooldown"`
	// 额度配置（从 Key 迁移到 Provider）
	Concurrency int   `yaml:"concurrency"`
	RPM         int   `yaml:"rpm"`
	TPM         int64 `yaml:"tpm"`
	TPH         int64 `yaml:"tph"`
	TPD         int64 `yaml:"tpd"`
	// Headers 是发往该上游的自定义请求头（同名单头覆盖默认值）。
	Headers map[string]string `yaml:"headers"`
	// PassHeaders 为 true 时透传客户端请求头（排除鉴权/内容类型等敏感头）。
	PassHeaders bool `yaml:"pass_headers"`
}

// Route 模型映射：统一模型名 -> 可用 providers 列表（按顺序优先）。
// 支持两种写法（可共存，Mappings 优先）：
//   - 简单模式 providers: [openai, azure] — 上游模型名 = 统一模型名（自动路由等价）
//   - 高级模式 mappings: [{provider: openai, upstream_model: gpt-4o}] — 重写上游模型名
type Route struct {
	Providers []string       `yaml:"providers"` // 简单模式：provider 名列表
	Mappings  []RouteMapping `yaml:"mappings"`  // 高级模式：provider + 上游模型名
}

// RouteMapping 是模型映射的高级条目：指定上游 Provider 和该 Provider 上的实际模型名。
type RouteMapping struct {
	Provider      string `yaml:"provider"`
	UpstreamModel string `yaml:"upstream_model"` // 空 = 与统一模型名相同
}

// KeyConfig 是 Harbor Key 定义（纯访问令牌，不含额度配置；P0 用明文 secret，写入时仅保存哈希）。
type KeyConfig struct {
	Secret        string   `yaml:"secret"`
	AllowedModels []string `yaml:"allowed_models"`
}

type RetryLimitsConfig struct {
	MaxProviderAttempts int    `yaml:"max_provider_attempts"`
	RequestTimeout      string `yaml:"request_timeout"`
	// StreamTTFBTimeout 流式请求等待上游响应头（TTFB）的上限，默认 60s。
	// 首包之后不再套固定总超时，改为按 stream_idle_timeout 判定空闲。
	StreamTTFBTimeout string `yaml:"stream_ttfb_timeout"`
	// StreamIdleTimeout 流转发期间两次上游数据之间的空闲上限，默认 120s；0 = 关闭。
	StreamIdleTimeout string `yaml:"stream_idle_timeout"`
	BackoffBase       string `yaml:"backoff_base"`
	ProviderCooldown  string `yaml:"provider_cooldown"`
}

type AdminConfig struct {
	Token string `yaml:"token"` // 管理接口 Bearer Token，空则关闭管理接口
	// Path 是管理台入口路径。默认 /harbor-console，必须显式避开 /admin、
	// /api、/v1 等常见或已占用路径；可在管理台设置页热改。
	Path string `yaml:"path"`
}

// PathValue 返回规范化后的管理台入口路径。
func (a AdminConfig) PathValue() string {
	path, err := NormalizeAdminPath(a.Path)
	if err != nil {
		return "/harbor-console"
	}
	return path
}

// NormalizeAdminPath 校验并规范化管理台入口路径。
func NormalizeAdminPath(raw string) (string, error) {
	path := strings.TrimSpace(raw)
	if path == "" {
		return "/harbor-console", nil
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	path = strings.TrimRight(path, "/")
	if path == "" {
		return "", fmt.Errorf("admin.path must not be root")
	}
	if len(path) > 128 {
		return "", fmt.Errorf("admin.path too long: %d characters (maximum 128)", len(path))
	}
	if strings.Contains(path, "//") {
		return "", fmt.Errorf("admin.path must not contain empty segments")
	}
	for _, seg := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if seg == "" || seg == "." || seg == ".." {
			return "", fmt.Errorf("admin.path contains invalid segment %q", seg)
		}
		for _, r := range seg {
			if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.') {
				return "", fmt.Errorf("admin.path may only contain letters, digits, '.', '-' and '_'")
			}
		}
	}
	lower := strings.ToLower(path)
	for _, reserved := range []string{"/admin", "/api", "/v1", "/landing.js"} {
		if lower == reserved || strings.HasPrefix(lower, reserved+"/") {
			return "", fmt.Errorf("admin.path must not use reserved prefix %q", reserved)
		}
	}
	return path, nil
}

type LogConfig struct {
	Level string `yaml:"level"` // debug | info | warn | error
	File  string `yaml:"file"`  // 可选日志文件
}

// Duration 解析结果（加载后填充）。
func (c *QueueConfig) MaxWaitDuration() time.Duration { return parseDur(c.MaxWait, 120*time.Second) }

// Early429AfterDuration 返回排队早退阈值；未配置返回 0（关闭）。
func (c *QueueConfig) Early429AfterDuration() time.Duration {
	if strings.TrimSpace(c.Early429After) == "" {
		return 0
	}
	return parseDur(c.Early429After, 0)
}
func (c *QueueConfig) KeepaliveDuration() time.Duration { return parseDur(c.Keepalive, 15*time.Second) }
func (c *ServerConfig) MaxBodySizeBytes() int64         { return parseBytes(c.MaxBodySize, 20<<20) }
func (c *ServerConfig) ReadTimeoutDuration() time.Duration {
	return parseDur(c.ReadTimeout, 30*time.Second)
}
func (c *ServerConfig) IdleTimeoutDuration() time.Duration {
	return parseDur(c.IdleTimeout, 120*time.Second)
}
func (c *ServerConfig) MaxBodyBytesInFlightBytes() int64 {
	return parseBytes(c.MaxBodyBytesInFlight, 256<<20)
}
func (c *RetryLimitsConfig) RequestTimeoutDuration() time.Duration {
	return parseDur(c.RequestTimeout, 120*time.Second)
}
func (c *RetryLimitsConfig) StreamTTFBTimeoutDuration() time.Duration {
	return parseDur(c.StreamTTFBTimeout, 60*time.Second)
}
func (c *RetryLimitsConfig) StreamIdleTimeoutDuration() time.Duration {
	return parseDur(c.StreamIdleTimeout, 120*time.Second)
}
func (c *RetryLimitsConfig) BackoffBaseDuration() time.Duration {
	return parseDur(c.BackoffBase, 500*time.Millisecond)
}
func (c *RetryLimitsConfig) ProviderCooldownDuration() time.Duration {
	return parseDur(c.ProviderCooldown, 30*time.Second)
}

// Load 读取并校验配置文件。
// 整个配置文件支持 ${ENV_VAR} 展开（PaaS 部署：监听端口/密钥/凭证/存储路径
// 全部可从环境变量注入）；未设置的变量保留原样，由校验给出清晰报错。
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return parse(data)
}

// Parse 从内存 YAML 文本解析配置（HARBOR_CONFIG 环境变量注入完整配置用）。
func Parse(data []byte) (*Config, error) {
	return parse(data)
}

// parse 解析配置内容：先做文本级 ${ENV} 展开，再 Unmarshal + 校验。
func parse(data []byte) (*Config, error) {
	data = expandTextEnv(data)
	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if err := cfg.checkEnvResidue(); err != nil {
		return nil, err
	}
	cfg.applyDefaults()
	return cfg, nil
}

var envRe = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// expandTextEnv 对整个配置文本做 ${ENV} 替换；未设置的变量保留原样，
// 残留由 checkEnvResidue 报错（避免静默注入空字符串导致诡异行为）。
func expandTextEnv(data []byte) []byte {
	return envRe.ReplaceAllFunc(data, func(m []byte) []byte {
		name := strings.TrimSuffix(strings.TrimPrefix(string(m), "${"), "}")
		if v, ok := os.LookupEnv(name); ok {
			return []byte(v)
		}
		return m
	})
}

// checkEnvResidue 检查关键字段是否有未展开的 ${ENV} 残留（= 环境变量未设置）。
func (c *Config) checkEnvResidue() error {
	if envRe.MatchString(c.Server.Listen) {
		return fmt.Errorf("server.listen references unset env var: %q (PaaS 部署请注入 PORT)", c.Server.Listen)
	}
	for i := range c.Providers {
		for j := range c.Providers[i].Credentials {
			raw := c.Providers[i].Credentials[j]
			if envRe.MatchString(raw) {
				return fmt.Errorf("provider %q credential %d (%q) references unset env var; set it or use a literal value", c.Providers[i].Name, j, raw)
			}
			if raw == "" {
				return fmt.Errorf("provider %q credential %d is empty", c.Providers[i].Name, j)
			}
		}
	}
	for i, k := range c.Keys {
		if envRe.MatchString(k.Secret) {
			return fmt.Errorf("keys[%d].secret references unset env var: %q", i, k.Secret)
		}
	}
	if envRe.MatchString(c.Admin.Token) {
		return fmt.Errorf("admin.token references unset env var: %q", c.Admin.Token)
	}
	if envRe.MatchString(c.Storage.DBPath) || envRe.MatchString(c.Storage.KeyPath) {
		return fmt.Errorf("storage paths reference unset env var: db_path=%q key_path=%q", c.Storage.DBPath, c.Storage.KeyPath)
	}
	return nil
}

func (c *Config) validate() error {
	if c.Server.Listen == "" {
		c.Server.Listen = "0.0.0.0:8080"
	}
	if _, _, err := net.SplitHostPort(c.Server.Listen); err != nil {
		return fmt.Errorf("server.listen invalid: %w", err)
	}
	if err := c.validateBounds(); err != nil {
		return err
	}
	if len(c.Providers) == 0 {
		return fmt.Errorf("at least one provider is required")
	}
	seen := map[string]bool{}
	for _, p := range c.Providers {
		if p.Name == "" {
			return fmt.Errorf("provider name required")
		}
		if seen[p.Name] {
			return fmt.Errorf("duplicate provider name %q", p.Name)
		}
		seen[p.Name] = true
		if p.Type == "" {
			return fmt.Errorf("provider %q type required (openai-compatible)", p.Name)
		}
		if p.BaseURL == "" {
			return fmt.Errorf("provider %q base_url required", p.Name)
		}
		if err := validateProviderURL(p.BaseURL); err != nil {
			return fmt.Errorf("provider %q base_url invalid: %w", p.Name, err)
		}
		if len(p.Credentials) == 0 {
			return fmt.Errorf("provider %q needs at least one credential", p.Name)
		}
		if p.Concurrency < 0 || p.RPM < 0 || p.TPM < 0 || p.TPH < 0 || p.TPD < 0 {
			return fmt.Errorf("provider %q limits must be >= 0", p.Name)
		}
	}
	for model, route := range c.Routes {
		if len(route.Providers) == 0 && len(route.Mappings) == 0 {
			return fmt.Errorf("route %q has no providers or mappings", model)
		}
		for _, pn := range route.Providers {
			if !seen[pn] {
				return fmt.Errorf("route %q references unknown provider %q", model, pn)
			}
		}
		for _, m := range route.Mappings {
			if !seen[m.Provider] {
				return fmt.Errorf("route %q mapping references unknown provider %q", model, m.Provider)
			}
		}
	}
	if len(c.Keys) == 0 {
		return fmt.Errorf("at least one Harbor key is required")
	}
	for i, k := range c.Keys {
		if k.Secret == "" {
			return fmt.Errorf("keys[%d].secret required", i)
		}
	}
	if c.Queue.MaxDepth <= 0 {
		c.Queue.MaxDepth = 200
	}
	if c.Admin.Path != "" {
		if _, err := NormalizeAdminPath(c.Admin.Path); err != nil {
			return fmt.Errorf("admin.path invalid: %w", err)
		}
	}
	return nil
}

func validateProviderURL(raw string) error {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("scheme must be http or https")
	}
	if u.Hostname() == "" {
		return fmt.Errorf("host is required")
	}
	if u.User != nil {
		return fmt.Errorf("user info is not allowed")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("query and fragment are not allowed")
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return fmt.Errorf("port is invalid")
		}
	}
	return nil
}

func (c *Config) validateBounds() error {
	for _, item := range []struct {
		name      string
		raw       string
		allowZero bool
	}{
		{"server.read_timeout", c.Server.ReadTimeout, false},
		{"server.idle_timeout", c.Server.IdleTimeout, true},
		{"queue.max_wait", c.Queue.MaxWait, false},
		{"queue.early_429_after", c.Queue.Early429After, true},
		{"queue.keepalive", c.Queue.Keepalive, false},
		{"limits.request_timeout", c.Limits.RequestTimeout, false},
		{"limits.stream_ttfb_timeout", c.Limits.StreamTTFBTimeout, false},
		{"limits.stream_idle_timeout", c.Limits.StreamIdleTimeout, true},
		{"limits.backoff_base", c.Limits.BackoffBase, true},
		{"limits.provider_cooldown", c.Limits.ProviderCooldown, true},
		{"keepalive.interval", c.Keepalive.Interval, false},
	} {
		if err := validateDuration(item.name, item.raw, item.allowZero); err != nil {
			return err
		}
	}
	if early := c.Queue.Early429AfterDuration(); early > 0 && early >= c.Queue.MaxWaitDuration() {
		return fmt.Errorf("queue.early_429_after must be shorter than queue.max_wait")
	}
	if c.Keepalive.Interval != "" {
		if d := c.Keepalive.IntervalDuration(); d < time.Second || d > 5*time.Minute {
			return fmt.Errorf("keepalive.interval must be between 1s and 5m, got %s", c.Keepalive.Interval)
		}
	}
	if s := strings.ToLower(strings.TrimSpace(c.Keepalive.Style)); s != "" && s != "comment" && s != "event" && s != "data" {
		return fmt.Errorf("keepalive.style must be comment or event, got %q", c.Keepalive.Style)
	}
	if len(c.Keepalive.Comment) > 64 {
		return fmt.Errorf("keepalive.comment too long: %d characters (maximum 64)", len(c.Keepalive.Comment))
	}
	for _, item := range []struct {
		name string
		raw  string
	}{
		{"server.max_body_size", c.Server.MaxBodySize},
		{"server.max_body_bytes_in_flight", c.Server.MaxBodyBytesInFlight},
	} {
		if item.raw == "" {
			continue
		}
		value, err := parseBytesStrict(item.raw)
		if err != nil {
			return fmt.Errorf("%s invalid: %w", item.name, err)
		}
		if value > 8<<30 {
			return fmt.Errorf("%s too large: %d bytes (maximum 8GB)", item.name, value)
		}
	}
	if c.Server.MaxBodyBytesInFlightBytes() < c.Server.MaxBodySizeBytes() {
		return fmt.Errorf("server.max_body_bytes_in_flight must be at least server.max_body_size")
	}
	if c.Queue.MaxDepth <= 0 {
		c.Queue.MaxDepth = 200
	}
	if c.Queue.MaxDepth > 10000 {
		return fmt.Errorf("queue.max_depth too large: %d (maximum 10000)", c.Queue.MaxDepth)
	}
	if c.Limits.MaxProviderAttempts > 100 {
		return fmt.Errorf("limits.max_provider_attempts too large: %d (maximum 100)", c.Limits.MaxProviderAttempts)
	}
	return nil
}

func validateDuration(name, raw string, allowZero bool) error {
	if raw == "" {
		return nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return fmt.Errorf("%s invalid: %w", name, err)
	}
	if (!allowZero && d <= 0) || (allowZero && d < 0) {
		return fmt.Errorf("%s must be %s", name, map[bool]string{true: ">= 0", false: "> 0"}[allowZero])
	}
	return nil
}

func (c *Config) applyDefaults() {
	if c.Limits.MaxProviderAttempts <= 0 {
		c.Limits.MaxProviderAttempts = 3
	}
	if c.Server.MaxHeaderBytes <= 0 {
		c.Server.MaxHeaderBytes = 1 << 20
	}
	c.Admin.Path = c.Admin.PathValue()
}

func parseDur(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return def
	}
	return d
}

func parseBytes(s string, def int64) int64 {
	if s == "" {
		return def
	}
	value, err := parseBytesStrict(s)
	if err != nil {
		return def
	}
	return value
}

func parseBytesStrict(raw string) (int64, error) {
	s := strings.TrimSpace(strings.ToLower(raw))
	if s == "" {
		return 0, fmt.Errorf("value is empty")
	}
	mult := int64(1)
	switch {
	case strings.HasSuffix(s, "kb"):
		mult, s = 1<<10, strings.TrimSuffix(s, "kb")
	case strings.HasSuffix(s, "mb"):
		mult, s = 1<<20, strings.TrimSuffix(s, "mb")
	case strings.HasSuffix(s, "gb"):
		mult, s = 1<<30, strings.TrimSuffix(s, "gb")
	case strings.HasSuffix(s, "b"):
		s = strings.TrimSuffix(s, "b")
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("must be a positive byte size")
	}
	if n <= 0 || n > (int64(1<<63-1))/mult {
		return 0, fmt.Errorf("must be a positive byte size")
	}
	return n * mult, nil
}
