package server

import (
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"harbor/internal/config"
	"harbor/internal/db"
)

// 运行时设置：可热改的项落库并在重启后重放；只读项与环境变量只做展示。

const (
	settingKeepalive = "keepalive" // 旧版单键，仅读取兼容
	settingRuntime   = "runtime"
)

// storedRuntime 是落库的运行时覆盖（只存被改过的字段）。
type storedRuntime struct {
	QueueMaxWaitMS      *int64           `json:"queue_max_wait_ms,omitempty"`
	QueueMaxDepth       *int             `json:"queue_max_depth,omitempty"`
	Early429MS          *int64           `json:"early_429_ms,omitempty"`
	ExposeQueuePosition *bool            `json:"expose_queue_position,omitempty"`
	RequestTimeoutMS    *int64           `json:"request_timeout_ms,omitempty"`
	MaxProviderAttempts *int             `json:"max_provider_attempts,omitempty"`
	BackoffBaseMS       *int64           `json:"backoff_base_ms,omitempty"`
	ProviderCooldownMS  *int64           `json:"provider_cooldown_ms,omitempty"`
	Keepalive           *storedKeepalive `json:"keepalive,omitempty"`
}

// storedKeepalive 是心跳设置的持久化形态。
type storedKeepalive struct {
	Enabled  *bool  `json:"enabled,omitempty"`
	Interval string `json:"interval,omitempty"`
	Style    string `json:"style,omitempty"`
	Comment  string `json:"comment,omitempty"`
}

// settingField 是设置页的一行。
type settingField struct {
	Group    string `json:"group"`
	Key      string `json:"key"`
	Label    string `json:"label"`
	Kind     string `json:"kind"` // seconds | ms | int | bool | text | read
	Value    any    `json:"value"`
	Editable bool   `json:"editable"`
	Source   string `json:"source"` // env | yaml
	Note     string `json:"note,omitempty"`
}

// envVar 是环境变量视图（敏感值只显示是否已设置）。
type envVar struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Set    bool   `json:"set"`
	Secret bool   `json:"secret"`
}

var envWhitelist = []struct {
	name   string
	secret bool
}{
	{"PORT", false},
	{"HARBOR_CONFIG", true},
	{"HARBOR_DB_PATH", false},
	{"HARBOR_KEY_PATH", false},
	{"HARBOR_KEY", true},
	{"HARBOR_ADMIN_TOKEN", true},
	{"OPENAI_API_KEY", true},
	{"FLY_APP_NAME", false},
	{"FLY_REGION", false},
	{"FLY_MACHINE_ID", false},
}

func cfgSource() string {
	if strings.TrimSpace(os.Getenv("HARBOR_CONFIG")) != "" {
		return "env"
	}
	return "yaml"
}

// keepaliveView 是给管理台的展示形态（兼容旧接口）。
type keepaliveView struct {
	Enabled      bool   `json:"enabled"`
	IntervalMS   int64  `json:"interval_ms"`
	IntervalText string `json:"interval_text"`
	Style        string `json:"style"`
	Comment      string `json:"comment"`
	FramePreview string `json:"frame_preview"`
}

func keepaliveViewOf(ka config.KeepaliveConfig) keepaliveView {
	comment := strings.TrimSpace(ka.Comment)
	if comment == "" {
		comment = "keepalive"
	}
	return keepaliveView{
		Enabled:      ka.IsEnabled(),
		IntervalMS:   ka.IntervalDuration().Milliseconds(),
		IntervalText: ka.IntervalDuration().String(),
		Style:        ka.StyleValue(),
		Comment:      comment,
		FramePreview: ka.Frame(),
	}
}

// buildSettingFields 汇总当前生效值：可热改的在前，只读的在后。
func (s *Server) buildSettingFields() []settingField {
	s.cfgMu.RLock()
	cfg := s.cfg
	fields := []settingField{
		{Group: "queue", Key: "queue_max_wait_ms", Label: "排队最长等待", Kind: "seconds", Value: cfg.Queue.MaxWaitDuration().Milliseconds(), Editable: true, Source: cfgSource(), Note: "超过该时长仍未调度则返回 429。"},
		{Group: "queue", Key: "queue_max_depth", Label: "队列容量", Kind: "int", Value: cfg.Queue.MaxDepth, Editable: true, Source: cfgSource(), Note: "排队请求上限，满则直接 429。"},
		{Group: "queue", Key: "expose_queue_position", Label: "返回排队位置", Kind: "bool", Value: cfg.Queue.ExposeQueuePosition, Editable: true, Source: cfgSource(), Note: "响应头 X-Queue-Position。"},
		{Group: "queue", Key: "early_429_ms", Label: "排队早退阈值", Kind: "seconds", Value: cfg.Queue.Early429AfterDuration().Milliseconds(), Editable: true, Source: cfgSource(), Note: "排队超过该时长仍未调度直接返回 429；0 = 关闭。"},
		{Group: "limits", Key: "request_timeout_ms", Label: "请求总超时", Kind: "seconds", Value: cfg.Limits.RequestTimeoutDuration().Milliseconds(), Editable: true, Source: cfgSource(), Note: "覆盖读体、排队与执行。"},
		{Group: "limits", Key: "max_provider_attempts", Label: "上游最大尝试", Kind: "int", Value: cfg.Limits.MaxProviderAttempts, Editable: true, Source: cfgSource(), Note: "单个请求累计发送上限，重排不重置。"},
		{Group: "limits", Key: "backoff_base_ms", Label: "重试退避基数", Kind: "ms", Value: cfg.Limits.BackoffBaseDuration().Milliseconds(), Editable: true, Source: cfgSource(), Note: "指数退避的起点。"},
		{Group: "limits", Key: "provider_cooldown_ms", Label: "上游冷却时长", Kind: "seconds", Value: cfg.Limits.ProviderCooldownDuration().Milliseconds(), Editable: true, Source: cfgSource(), Note: "失败后该凭证的冷却时间。"},
	}
	ka := s.keepaliveConfig()
	fields = append(fields,
		settingField{Group: "keepalive", Key: "keepalive_enabled", Label: "启用心跳保活", Kind: "bool", Value: ka.IsEnabled(), Editable: true, Source: cfgSource(), Note: "SSE / 流式响应静默时补空事件。"},
		settingField{Group: "keepalive", Key: "keepalive_interval_ms", Label: "空闲间隔", Kind: "seconds", Value: ka.IntervalDuration().Milliseconds(), Editable: true, Source: cfgSource(), Note: "1–300 秒。"},
		settingField{Group: "keepalive", Key: "keepalive_style", Label: "发送形式", Kind: "text", Value: ka.StyleValue(), Editable: true, Source: cfgSource(), Note: "comment = SSE 注释行；event = data 事件。"},
		settingField{Group: "keepalive", Key: "keepalive_comment", Label: "注释文本", Kind: "text", Value: orDefault(strings.TrimSpace(ka.Comment), "keepalive"), Editable: true, Source: cfgSource(), Note: "仅 comment 形式使用，最长 64 字符。"},
	)
	fields = append(fields,
		settingField{Group: "readonly", Key: "listen", Label: "监听地址", Kind: "read", Value: cfg.Server.Listen, Source: "yaml"},
		settingField{Group: "readonly", Key: "max_body_size", Label: "请求体上限", Kind: "read", Value: orDefault(cfg.Server.MaxBodySize, "20mb"), Source: "yaml"},
		settingField{Group: "readonly", Key: "read_timeout", Label: "读超时", Kind: "read", Value: orDefault(cfg.Server.ReadTimeout, "30s"), Source: "yaml"},
		settingField{Group: "readonly", Key: "idle_timeout", Label: "空闲连接超时", Kind: "read", Value: orDefault(cfg.Server.IdleTimeout, "120s"), Source: "yaml"},
		settingField{Group: "readonly", Key: "max_body_in_flight", Label: "在途请求体预算", Kind: "read", Value: orDefault(cfg.Server.MaxBodyBytesInFlight, "256mb"), Source: "yaml"},
		settingField{Group: "readonly", Key: "db_path", Label: "SQLite 路径", Kind: "read", Value: orDefault(cfg.Storage.DBPath, "（纯内存）"), Source: "yaml"},
		settingField{Group: "readonly", Key: "key_path", Label: "主密钥路径", Kind: "read", Value: orDefault(cfg.Storage.KeyPath, "harbor.key"), Source: "yaml"},
		settingField{Group: "readonly", Key: "log_level", Label: "日志级别", Kind: "read", Value: orDefault(cfg.Log.Level, "info"), Source: "yaml"},
		settingField{Group: "readonly", Key: "log_file", Label: "日志文件", Kind: "read", Value: orDefault(cfg.Log.File, "（仅标准输出）"), Source: "yaml"},
	)
	adminState := "已关闭（未配置 token）"
	if strings.TrimSpace(cfg.Admin.Token) != "" {
		adminState = "已配置（不显示）"
	}
	s.cfgMu.RUnlock()
	return append(fields, settingField{Group: "readonly", Key: "admin_token", Label: "管理 token", Kind: "read", Value: adminState, Source: "yaml"})
}

func orDefault(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}

// envView 只暴露白名单变量；敏感值不返回内容。
func envView() []envVar {
	out := make([]envVar, 0, len(envWhitelist))
	for _, item := range envWhitelist {
		raw := os.Getenv(item.name)
		entry := envVar{Name: item.name, Set: strings.TrimSpace(raw) != "", Secret: item.secret}
		if entry.Set {
			if item.secret {
				entry.Value = "已设置（不显示）"
			} else {
				entry.Value = raw
			}
		} else {
			entry.Value = "—"
		}
		out = append(out, entry)
	}
	return out
}

// handleGetSettings GET /api/settings
func (s *Server) handleGetSettings(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"fields":     s.buildSettingFields(),
		"env":        envView(),
		"keepalive":  keepaliveViewOf(s.keepaliveConfig()),
		"persistent": s.store != nil,
	})
}

// settingsUpdate 是 PUT /api/settings 的请求体（全部可选）。
type settingsUpdate struct {
	QueueMaxWaitMS      *int64 `json:"queue_max_wait_ms"`
	QueueMaxDepth       *int   `json:"queue_max_depth"`
	Early429MS          *int64 `json:"early_429_ms"`
	ExposeQueuePosition *bool  `json:"expose_queue_position"`
	RequestTimeoutMS    *int64 `json:"request_timeout_ms"`
	MaxProviderAttempts *int   `json:"max_provider_attempts"`
	BackoffBaseMS       *int64 `json:"backoff_base_ms"`
	ProviderCooldownMS  *int64 `json:"provider_cooldown_ms"`
	Keepalive           *struct {
		Enabled    *bool   `json:"enabled"`
		IntervalMS *int64  `json:"interval_ms"`
		Style      *string `json:"style"`
		Comment    *string `json:"comment"`
	} `json:"keepalive"`
}

// handlePutSettings PUT /api/settings —— 校验、热应用、落库。
func (s *Server) handlePutSettings(w http.ResponseWriter, r *http.Request) {
	var req settingsUpdate
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16))
	if err := dec.Decode(&req); err != nil {
		s.writeError(w, s.newRequestID(), invalidRequestErr(err.Error()))
		return
	}
	if err := validateSettingsUpdate(&req); err != nil {
		s.writeError(w, s.newRequestID(), invalidRequestErr(err.Error()))
		return
	}

	s.cfgMu.Lock()
	if req.QueueMaxWaitMS != nil {
		s.cfg.Queue.MaxWait = time.Duration(*req.QueueMaxWaitMS * int64(time.Millisecond)).String()
	}
	if req.QueueMaxDepth != nil {
		s.cfg.Queue.MaxDepth = *req.QueueMaxDepth
	}
	if req.ExposeQueuePosition != nil {
		s.cfg.Queue.ExposeQueuePosition = *req.ExposeQueuePosition
	}
	if req.Early429MS != nil {
		if *req.Early429MS == 0 {
			s.cfg.Queue.Early429After = ""
		} else {
			s.cfg.Queue.Early429After = time.Duration(*req.Early429MS * int64(time.Millisecond)).String()
		}
	}
	if req.RequestTimeoutMS != nil {
		s.cfg.Limits.RequestTimeout = time.Duration(*req.RequestTimeoutMS * int64(time.Millisecond)).String()
	}
	if req.MaxProviderAttempts != nil {
		s.cfg.Limits.MaxProviderAttempts = *req.MaxProviderAttempts
	}
	if req.BackoffBaseMS != nil {
		s.cfg.Limits.BackoffBase = time.Duration(*req.BackoffBaseMS * int64(time.Millisecond)).String()
	}
	if req.ProviderCooldownMS != nil {
		s.cfg.Limits.ProviderCooldown = time.Duration(*req.ProviderCooldownMS * int64(time.Millisecond)).String()
	}
	ka := s.keepaliveConfig()
	if req.Keepalive != nil {
		if req.Keepalive.Enabled != nil {
			v := *req.Keepalive.Enabled
			ka.Enabled = &v
		}
		if req.Keepalive.IntervalMS != nil {
			ka.Interval = time.Duration(*req.Keepalive.IntervalMS * int64(time.Millisecond)).String()
		}
		if req.Keepalive.Style != nil {
			ka.Style = *req.Keepalive.Style
		}
		if req.Keepalive.Comment != nil {
			ka.Comment = strings.NewReplacer("\r", " ", "\n", " ").Replace(strings.TrimSpace(*req.Keepalive.Comment))
		}
		s.cfg.Keepalive = ka
	}
	s.cfgMu.Unlock()

	// 热应用：调度器、执行器、请求链路、心跳。
	s.sched.SetLimits(s.cfg.Queue.MaxDepth, s.cfg.Queue.MaxWaitDuration())
	s.sched.SetEarlyWait(s.cfg.Queue.Early429AfterDuration())
	s.exec.SetPolicy(s.cfg.Limits.MaxProviderAttempts, s.cfg.Limits.BackoffBaseDuration())
	s.setRuntimeLimits(s.cfg.Limits.RequestTimeoutDuration(), s.cfg.Queue.ExposeQueuePosition)
	s.SetKeepalive(ka)

	if s.store != nil {
		stored := s.mergeStoredRuntime(req)
		if payload, err := json.Marshal(stored); err == nil {
			if err := s.store.SetSetting(settingRuntime, string(payload)); err != nil {
				s.log.Warn("persist runtime settings failed", "err", err.Error())
			}
		}
	}
	s.log.Info("runtime settings updated",
		"queue_max_wait", s.cfg.Queue.MaxWait, "queue_max_depth", s.cfg.Queue.MaxDepth,
		"request_timeout", s.cfg.Limits.RequestTimeout, "max_attempts", s.cfg.Limits.MaxProviderAttempts,
		"keepalive", ka.IntervalDuration().String())
	writeJSON(w, http.StatusOK, map[string]any{
		"fields":    s.buildSettingFields(),
		"keepalive": keepaliveViewOf(s.keepaliveConfig()),
	})
}

// mergeStoredRuntime 在已有落库值上叠加本次修改（未改的字段保持原样）。
func (s *Server) mergeStoredRuntime(req settingsUpdate) storedRuntime {
	var stored storedRuntime
	if s.store != nil {
		if raw, err := s.store.GetSetting(settingRuntime); err == nil {
			_ = json.Unmarshal([]byte(raw), &stored)
		}
	}
	if req.QueueMaxWaitMS != nil {
		stored.QueueMaxWaitMS = req.QueueMaxWaitMS
	}
	if req.QueueMaxDepth != nil {
		stored.QueueMaxDepth = req.QueueMaxDepth
	}
	if req.Early429MS != nil {
		stored.Early429MS = req.Early429MS
	}
	if req.ExposeQueuePosition != nil {
		stored.ExposeQueuePosition = req.ExposeQueuePosition
	}
	if req.RequestTimeoutMS != nil {
		stored.RequestTimeoutMS = req.RequestTimeoutMS
	}
	if req.MaxProviderAttempts != nil {
		stored.MaxProviderAttempts = req.MaxProviderAttempts
	}
	if req.BackoffBaseMS != nil {
		stored.BackoffBaseMS = req.BackoffBaseMS
	}
	if req.ProviderCooldownMS != nil {
		stored.ProviderCooldownMS = req.ProviderCooldownMS
	}
	if req.Keepalive != nil {
		ka := s.keepaliveConfig()
		if stored.Keepalive == nil {
			stored.Keepalive = &storedKeepalive{}
		}
		enabled := ka.IsEnabled()
		stored.Keepalive.Enabled = &enabled
		stored.Keepalive.Interval = ka.Interval
		stored.Keepalive.Style = ka.StyleValue()
		stored.Keepalive.Comment = ka.Comment
	}
	return stored
}

func validateSettingsUpdate(req *settingsUpdate) error {
	check := func(name string, v *int64, min, max int64) error {
		if v == nil {
			return nil
		}
		if *v < min || *v > max {
			return errors.New(name + " 超出允许范围")
		}
		return nil
	}
	if err := check("queue_max_wait_ms", req.QueueMaxWaitMS, 1000, 3600000); err != nil {
		return err
	}
	if err := check("request_timeout_ms", req.RequestTimeoutMS, 1000, 3600000); err != nil {
		return err
	}
	if err := check("backoff_base_ms", req.BackoffBaseMS, 0, 60000); err != nil {
		return err
	}
	if err := check("provider_cooldown_ms", req.ProviderCooldownMS, 0, 3600000); err != nil {
		return err
	}
	if req.Early429MS != nil && (*req.Early429MS < 0 || *req.Early429MS > 1800000) {
		return errors.New("early_429_ms 需在 0–1800000 之间")
	}
	if req.QueueMaxDepth != nil && (*req.QueueMaxDepth < 1 || *req.QueueMaxDepth > 10000) {
		return errors.New("queue_max_depth 需在 1–10000 之间")
	}
	if req.MaxProviderAttempts != nil && (*req.MaxProviderAttempts < 1 || *req.MaxProviderAttempts > 100) {
		return errors.New("max_provider_attempts 需在 1–100 之间")
	}
	if req.Keepalive != nil {
		if req.Keepalive.IntervalMS != nil && (*req.Keepalive.IntervalMS < 1000 || *req.Keepalive.IntervalMS > 300000) {
			return errors.New("keepalive 间隔需在 1–300 秒之间")
		}
		if req.Keepalive.Style != nil {
			style := strings.ToLower(strings.TrimSpace(*req.Keepalive.Style))
			if style != "comment" && style != "event" {
				return errors.New("keepalive 形式只能是 comment 或 event")
			}
		}
		if req.Keepalive.Comment != nil && len(strings.TrimSpace(*req.Keepalive.Comment)) > 64 {
			return errors.New("keepalive 注释最长 64 字符")
		}
	}
	return nil
}

// handlePutKeepalive 保留旧接口（只改心跳）。
func (s *Server) handlePutKeepalive(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Enabled    *bool   `json:"enabled"`
		IntervalMS *int64  `json:"interval_ms"`
		Style      string  `json:"style"`
		Comment    *string `json:"comment"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16))
	if err := dec.Decode(&req); err != nil {
		s.writeError(w, s.newRequestID(), invalidRequestErr(err.Error()))
		return
	}
	update := settingsUpdate{Keepalive: &struct {
		Enabled    *bool   `json:"enabled"`
		IntervalMS *int64  `json:"interval_ms"`
		Style      *string `json:"style"`
		Comment    *string `json:"comment"`
	}{Enabled: req.Enabled, IntervalMS: req.IntervalMS, Comment: req.Comment}}
	if req.Style != "" {
		update.Keepalive.Style = &req.Style
	}
	if err := validateSettingsUpdate(&update); err != nil {
		s.writeError(w, s.newRequestID(), invalidRequestErr(err.Error()))
		return
	}
	ka := s.keepaliveConfig()
	if req.Enabled != nil {
		v := *req.Enabled
		ka.Enabled = &v
	}
	if req.IntervalMS != nil {
		ka.Interval = time.Duration(*req.IntervalMS * int64(time.Millisecond)).String()
	}
	if req.Style != "" {
		ka.Style = strings.ToLower(req.Style)
	}
	if req.Comment != nil {
		ka.Comment = strings.NewReplacer("\r", " ", "\n", " ").Replace(strings.TrimSpace(*req.Comment))
	}
	s.SetKeepalive(ka)
	s.cfgMu.Lock()
	s.cfg.Keepalive = ka
	s.cfgMu.Unlock()
	if s.store != nil {
		stored := s.mergeStoredRuntime(update)
		if payload, err := json.Marshal(stored); err == nil {
			_ = s.store.SetSetting(settingRuntime, string(payload))
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"keepalive": keepaliveViewOf(s.keepaliveConfig())})
}

// ApplyStoredSettings 启动时把落库的运行时设置叠加到配置（存储优先于 yaml）。
func ApplyStoredSettings(store *db.DB, cfg *config.Config, log *slog.Logger) {
	if store == nil || cfg == nil {
		return
	}
	applied := false

	if raw, err := store.GetSetting(settingRuntime); err == nil {
		var st storedRuntime
		if err := json.Unmarshal([]byte(raw), &st); err == nil {
			if st.QueueMaxWaitMS != nil {
				cfg.Queue.MaxWait = time.Duration(*st.QueueMaxWaitMS * int64(time.Millisecond)).String()
			}
			if st.QueueMaxDepth != nil {
				cfg.Queue.MaxDepth = *st.QueueMaxDepth
			}
			if st.Early429MS != nil {
				if *st.Early429MS == 0 {
					cfg.Queue.Early429After = ""
				} else {
					cfg.Queue.Early429After = time.Duration(*st.Early429MS * int64(time.Millisecond)).String()
				}
			}
			if st.ExposeQueuePosition != nil {
				cfg.Queue.ExposeQueuePosition = *st.ExposeQueuePosition
			}
			if st.RequestTimeoutMS != nil {
				cfg.Limits.RequestTimeout = time.Duration(*st.RequestTimeoutMS * int64(time.Millisecond)).String()
			}
			if st.MaxProviderAttempts != nil {
				cfg.Limits.MaxProviderAttempts = *st.MaxProviderAttempts
			}
			if st.BackoffBaseMS != nil {
				cfg.Limits.BackoffBase = time.Duration(*st.BackoffBaseMS * int64(time.Millisecond)).String()
			}
			if st.ProviderCooldownMS != nil {
				cfg.Limits.ProviderCooldown = time.Duration(*st.ProviderCooldownMS * int64(time.Millisecond)).String()
			}
			if st.Keepalive != nil {
				applyStoredKeepalive(cfg, *st.Keepalive)
			}
			applied = true
		} else if log != nil {
			log.Warn("decode stored runtime settings failed", "err", err.Error())
		}
	}

	// 旧版单键心跳兼容。
	if raw, err := store.GetSetting(settingKeepalive); err == nil {
		var st storedKeepalive
		if err := json.Unmarshal([]byte(raw), &st); err == nil {
			applyStoredKeepalive(cfg, st)
			applied = true
		}
	} else if err != nil && !errors.Is(err, sql.ErrNoRows) && log != nil {
		log.Warn("read stored settings failed", "err", err.Error())
	}

	if applied && log != nil {
		log.Info("stored settings applied",
			"queue_max_wait", cfg.Queue.MaxWaitDuration().String(),
			"queue_max_depth", cfg.Queue.MaxDepth,
			"request_timeout", cfg.Limits.RequestTimeoutDuration().String(),
			"keepalive_enabled", cfg.Keepalive.IsEnabled(),
			"keepalive_interval", cfg.Keepalive.IntervalDuration().String())
	}
}

func applyStoredKeepalive(cfg *config.Config, st storedKeepalive) {
	if st.Enabled != nil {
		cfg.Keepalive.Enabled = st.Enabled
	}
	if st.Interval != "" {
		cfg.Keepalive.Interval = st.Interval
	}
	if st.Style != "" {
		cfg.Keepalive.Style = st.Style
	}
	if st.Comment != "" {
		cfg.Keepalive.Comment = st.Comment
	}
}
