// Package server 提供 Harbor 的 HTTP 服务：
// OpenAI 兼容接口（chat/completions、responses、embeddings、models）、
// 鉴权、排队挂起、SSE 心跳与统一错误输出。
package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"harbor/internal/compat"
	"harbor/internal/config"
	"harbor/internal/db"
	"harbor/internal/executor"
	"harbor/internal/key"
	"harbor/internal/limits"
	"harbor/internal/normalize"
	"harbor/internal/provider"
	"harbor/internal/queue"
	"harbor/internal/stats"
)

// Server 是 HTTP 服务。
type Server struct {
	cfg   *config.Config
	keys  *key.Store
	mgr   *provider.Manager
	exec  *executor.Executor
	sched *queue.Scheduler
	stats *stats.Stats
	log   *slog.Logger
	// store 是 SQLite 持久化镜像（P2）；nil = 纯内存模式。
	// 热路径（鉴权/排队/执行）不经过 store，仅在请求结束时异步镜像。
	store *db.DB

	encOnce sync.Once
	encKey  []byte
	encErr  error

	bodyBudget *byteBudget

	// ka 是长连接心跳保活配置（管理台可热更新）。
	kaMu sync.RWMutex
	ka   config.KeepaliveConfig

	// cfgMu 保护可热更新的配置字段（管理台设置页读写）。
	cfgMu sync.RWMutex
	// 热更新后的请求总超时与排队位置头开关。
	requestTimeoutNs atomic.Int64
	exposeQueuePos   atomic.Bool
	// 流式链路独立超时：上游响应头（TTFB）与流转发空闲读。
	streamTTFBNs atomic.Int64
	streamIdleNs atomic.Int64

	reqSeq atomic.Uint64

	gateMu  sync.Mutex
	gateMap map[string]*gateSet // provider name -> gates

	recentMu    sync.Mutex
	recent      map[string]*RequestLog
	recentOrder []string
}

// gateSet 是一个 Provider 的全部额度 Gate。
type gateSet struct {
	concurrency *limits.ConcurrencyGate
	rpm         *limits.RateGate
	token       *limits.TokenGate
}

// RequestLog 是单请求记录（内存版，P2 迁 SQLite）。
type RequestLog struct {
	RequestID string `json:"request_id"`
	KeyID     string `json:"key_id"`
	Model     string `json:"model"`
	Provider  string `json:"provider,omitempty"` // 实际命中的上游 Provider；校验类失败为空
	Status    string `json:"status"`             // queued | connecting | waiting | responding | success | error | timeout | cancelled
	ErrorCode string `json:"error_code,omitempty"`
	// UpstreamCode/UpstreamType/UpstreamHTTP 上游原始错误信息（仅后台审计展示，绝不对外）。
	UpstreamCode  string    `json:"upstream_code,omitempty"`
	UpstreamType  string    `json:"upstream_type,omitempty"`
	UpstreamHTTP  int       `json:"upstream_http,omitempty"`
	ErrorDetail   string    `json:"error_detail,omitempty"` // 内部原因（仅后台审计展示）
	QueuedAt      time.Time `json:"queued_at"`
	StartedAt     time.Time `json:"started_at,omitempty"`
	FinishedAt    time.Time `json:"finished_at"`
	QueueWaitMs   int64     `json:"queue_wait_ms"`
	Attempts      int       `json:"attempts"`
	TokensIn      int       `json:"tokens_in"`
	TokensOut     int       `json:"tokens_out"`
	Stream        bool      `json:"stream"`
	ClientUA      string    `json:"client_ua,omitempty"`
	ClientIP      string    `json:"client_ip,omitempty"`
	UpstreamModel string    `json:"upstream_model,omitempty"`
	FirstTokenMs  int64     `json:"first_token_ms,omitempty"`
	// 上游元信息（2026-11 新增）
	ResponseModel     string `json:"response_model,omitempty"`
	UpstreamRequestID string `json:"upstream_request_id,omitempty"`
	CachedTokens      int    `json:"cached_tokens,omitempty"`
	CachedWriteTokens int    `json:"cache_write_tokens,omitempty"`
	// 链路观测（2026-11 新增）
	InboundPath         string `json:"inbound_path,omitempty"`          // 真实入站路径
	UpstreamHeaderMs    int64  `json:"upstream_header_ms,omitempty"`    // 上游响应头耗时
	UpstreamContentType string `json:"upstream_content_type,omitempty"` // 上游 Content-Type
	// ThinkingLevel 是本次请求的思考等级（low / medium / high / max 等）。
	ThinkingLevel string `json:"thinking_level,omitempty"`
}

// New 构造 Server。store 为 nil 时运行纯内存模式（P0 行为）。
func New(cfg *config.Config, keys *key.Store, mgr *provider.Manager, exec *executor.Executor, sched *queue.Scheduler, st *stats.Stats, store *db.DB, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	// 兼容旧配置：keepalive.interval 未显式配置时沿用 queue.keepalive。
	ka := cfg.Keepalive
	if strings.TrimSpace(ka.Interval) == "" && strings.TrimSpace(cfg.Queue.Keepalive) != "" {
		ka.Interval = cfg.Queue.Keepalive
	}
	s := &Server{
		cfg:        cfg,
		keys:       keys,
		mgr:        mgr,
		exec:       exec,
		sched:      sched,
		stats:      st,
		log:        log,
		store:      store,
		bodyBudget: newByteBudget(cfg.Server.MaxBodyBytesInFlightBytes()),
		gateMap:    map[string]*gateSet{},
		recent:     map[string]*RequestLog{},
		ka:         ka,
	}
	sched.SetEarlyWait(cfg.Queue.Early429AfterDuration())
	sched.OnFinished = s.onFinished
	s.requestTimeoutNs.Store(int64(cfg.Limits.RequestTimeoutDuration()))
	s.streamTTFBNs.Store(int64(cfg.Limits.StreamTTFBTimeoutDuration()))
	s.streamIdleNs.Store(int64(cfg.Limits.StreamIdleTimeoutDuration()))
	s.exposeQueuePos.Store(cfg.Queue.ExposeQueuePosition)
	return s
}

// Handler 返回 HTTP Handler（含中间件）。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", s.handleOpenAI("/v1/chat/completions"))
	mux.HandleFunc("POST /v1/responses", s.handleOpenAI("/v1/responses"))
	mux.HandleFunc("POST /v1/embeddings", s.handleOpenAI("/v1/embeddings"))
	mux.HandleFunc("POST /v1/messages", s.handleOpenAI("/v1/messages"))
	mux.HandleFunc("GET /v1/models", s.handleModels)
	s.registerAdmin(mux)
	s.registerPublic(mux)
	s.registerUI(mux)

	var h http.Handler = mux
	h = s.recoverMiddleware(h)
	h = s.logMiddleware(h)
	h = s.securityHeadersMiddleware(h)
	return h
}

// --- OpenAI 兼容代理 ---

type openAIReqMeta struct {
	Model           string          `json:"model"`
	Stream          bool            `json:"stream"`
	ReasoningEffort string          `json:"reasoning_effort"`
	Reasoning       json.RawMessage `json:"reasoning"`
	Thinking        json.RawMessage `json:"thinking"`
}

// normalizeThinkingLevel 只接受已知思考等级，避免把任意字符串写进审计。
func normalizeThinkingLevel(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "minimal", "low", "medium", "high", "max", "none", "auto":
		return strings.ToLower(strings.TrimSpace(v))
	default:
		return ""
	}
}

// thinkingLevelFromModel 解析模型名尾部的思考等级，如 deepseek-v4.1-flash(max)。
func thinkingLevelFromModel(model string) string {
	runes := []rune(strings.TrimSpace(model))
	if len(runes) < 3 {
		return ""
	}
	last := runes[len(runes)-1]
	var open rune
	switch last {
	case ')':
		open = '('
	case ']':
		open = '['
	case '）':
		open = '（'
	case '】':
		open = '【'
	default:
		return ""
	}
	for i := len(runes) - 2; i >= 0; i-- {
		if runes[i] == open {
			return normalizeThinkingLevel(string(runes[i+1 : len(runes)-1]))
		}
	}
	return ""
}

// thinkingLevelOf 从请求体提取思考等级：
// chat 的 reasoning_effort、Responses 的 reasoning.effort、DeepSeek 的 thinking，
// 最后回退到模型名后缀（max）约定。
func thinkingLevelOf(meta openAIReqMeta, model string) string {
	if level := normalizeThinkingLevel(meta.ReasoningEffort); level != "" {
		return level
	}
	if len(meta.Reasoning) > 0 {
		var r struct {
			Effort string `json:"effort"`
		}
		if json.Unmarshal(meta.Reasoning, &r) == nil {
			if level := normalizeThinkingLevel(r.Effort); level != "" {
				return level
			}
		}
	}
	if len(meta.Thinking) > 0 {
		var t struct {
			Level string `json:"level"`
			Type  string `json:"type"`
		}
		if json.Unmarshal(meta.Thinking, &t) == nil {
			if level := normalizeThinkingLevel(t.Level); level != "" {
				return level
			}
			if level := normalizeThinkingLevel(t.Type); level != "" {
				return level
			}
		}
		var plain string
		if json.Unmarshal(meta.Thinking, &plain) == nil {
			if level := normalizeThinkingLevel(plain); level != "" {
				return level
			}
		}
	}
	return thinkingLevelFromModel(model)
}

func rejectDuplicateTopLevelFields(body []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		return nil
	}
	seen := map[string]struct{}{}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		name, ok := token.(string)
		if !ok {
			return fmt.Errorf("invalid JSON object field")
		}
		if _, exists := seen[name]; exists {
			return fmt.Errorf("duplicate JSON field %q", name)
		}
		seen[name] = struct{}{}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return err
		}
	}
	_, err = decoder.Token()
	return err
}

// handleOpenAI 处理 OpenAI 兼容的上游转发（chat/completions 等），
// 经过：鉴权 → 校验 → 协议桥 → 排队 → 挂起 → 执行 → 收敛返回。
// /v1/responses、/v1/messages 会先翻成 /v1/chat/completions 再打 openai-compatible 上游。
func (s *Server) handleOpenAI(inboundPath string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requestID := s.newRequestID()
		w.Header().Set("X-Request-Id", requestID)
		start := time.Now()

		// 1. 鉴权
		k, err := s.keys.Authenticate(r.Header.Get("Authorization"))
		if err != nil {
			s.writeAuthError(w, requestID, err)
			s.log.Warn("auth failed", "request_id", requestID, "err", err.Error(), "remote", r.RemoteAddr)
			return
		}

		// 2. 读取阶段的预算：非流式请求复用这个 context 作为整段总超时；
		//    流式请求在解析出 stream 标志后立即取消它，避免总超时截断长流。
		readCtx, readCancel := context.WithTimeout(r.Context(), time.Duration(s.requestTimeoutNs.Load()))
		defer readCancel()

		// 3. 读取受限请求体，并用全局字节预算限制驻留内存。
		maxBody := s.cfg.Server.MaxBodySizeBytes()
		if r.ContentLength > maxBody {
			s.writeError(w, requestID, normalize.New(normalize.ErrInvalidRequest, "request body too large"))
			return
		}
		reservation := maxBody
		if r.ContentLength >= 0 && r.ContentLength < reservation {
			reservation = r.ContentLength
		}
		reservedUnits, ok := s.bodyBudget.Acquire(readCtx, reservation)
		if !ok {
			s.writeError(w, requestID, normalize.New(normalize.ErrRateLimited, "request body capacity exhausted"))
			return
		}
		var releaseBodyOnce sync.Once
		releaseBody := func() { releaseBodyOnce.Do(func() { s.bodyBudget.Release(reservedUnits) }) }
		var handedOff atomic.Bool
		defer func() {
			if !handedOff.Load() {
				releaseBody()
			}
		}()
		r.Body = http.MaxBytesReader(w, r.Body, maxBody)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			s.writeError(w, requestID, normalize.New(normalize.ErrInvalidRequest, fmt.Sprintf("read body: %v", err)))
			return
		}

		// 4. 元信息解析
		if err := rejectDuplicateTopLevelFields(body); err != nil {
			s.writeError(w, requestID, normalize.New(normalize.ErrInvalidRequest, err.Error()))
			return
		}
		var meta openAIReqMeta
		if err := json.Unmarshal(body, &meta); err != nil {
			s.writeError(w, requestID, normalize.New(normalize.ErrInvalidRequest, fmt.Sprintf("invalid json: %v", err)))
			return
		}
		if meta.Model == "" {
			s.writeError(w, requestID, normalize.New(normalize.ErrInvalidRequest, "model is required"))
			return
		}
		thinkingLevel := thinkingLevelOf(meta, meta.Model)
		// 兼容没写 body.stream 的客户端：Accept: text/event-stream 或 ?stream=true
		// 也视为流式请求。否则网关会按普通 JSON 返回，客户端只能等整段生成完才看到内容。
		if !meta.Stream {
			via := ""
			switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("stream"))) {
			case "1", "true", "yes":
				via = "query"
			}
			if via == "" && strings.Contains(strings.ToLower(r.Header.Get("Accept")), "text/event-stream") {
				via = "accept"
			}
			if via != "" {
				if rewritten, err := withStreamFlag(body, true); err == nil {
					body = rewritten
					meta.Stream = true
					s.log.Debug("stream inferred", "request_id", requestID, "via", via)
				}
			}
		}

		upstreamPath, body, bridge, err := compat.RewriteRequest(inboundPath, body)
		if err != nil {
			s.writeError(w, requestID, normalize.New(normalize.ErrInvalidRequest, err.Error()))
			return
		}

		// 4. 超时模型：
		//   - 非流式：保留 requestTimeout 总超时（覆盖排队 + 执行 + 读响应）；
		//   - 流式：只继承 r.Context()，由排队超时（queue.max_wait）、
		//     上游 TTFB（provider.Execute）与流转发空闲超时（copyStream）分段控制。
		//     绝不套固定总超时，否则长回答会在生成中途被截断。
		ctx := r.Context()
		cancel := func() {}
		if !meta.Stream {
			// 非流式沿用读取阶段创建的总超时（覆盖排队 + 执行 + 读响应）。
			ctx, cancel = readCtx, readCancel
		} else {
			// 流式显式结束读取阶段的计时器；之后由排队/TTFB/空闲超时控制。
			readCancel()
		}
		defer cancel()

		// 5. 模型校验（路由存在 + Key 允许）
		if len(s.mgr.ModelProviders(meta.Model)) == 0 {
			s.record(r, k, requestID, meta.Model, meta.Stream, "error", "model_forbidden", start, 0, 0, 0)
			s.writeError(w, requestID, normalize.New(normalize.ErrModelForbidden, "model "+meta.Model+" has no route"))
			return
		}
		if !k.ModelAllowed(meta.Model) {
			s.record(r, k, requestID, meta.Model, meta.Stream, "error", "model_forbidden", start, 0, 0, 0)
			s.writeError(w, requestID, normalize.New(normalize.ErrModelForbidden, "model "+meta.Model+" not allowed for key"))
			return
		}

		// 6. 构造上游请求（responses/messages 已桥成 chat/completions）
		upReq := &provider.Request{
			Path:        upstreamPath,
			Body:        body,
			Stream:      meta.Stream,
			TTFBTimeout: time.Duration(s.streamTTFBNs.Load()),
			PhaseHook: func(phase string) {
				s.setLiveStatus(requestID, phase)
			},
		}

		lease := s.newProviderLease(meta.Model)
		if lease == nil {
			s.writeError(w, requestID, normalize.New(normalize.ErrModelForbidden, "model "+meta.Model+" has no route"))
			return
		}
		s.beginLiveRequest(r, k, requestID, meta.Model, meta.Stream, start, thinkingLevel)

		// 每次出队只执行一个 attempt；这个闭包跨重排保留已使用的预算。
		attempts := 0
		job := &queue.Job{
			ID:               requestID,
			KeyHash:          k.Hash,
			Model:            meta.Model,
			Priority:         0, // Key 不再携带优先级，统一 FIFO
			Ctx:              ctx,
			Cancel:           cancel,
			TokenReserve:     estimatePromptTokens(body),
			ReleaseResources: releaseBody,
			Do: func(ctx context.Context) *queue.Result {
				if p := lease.Provider(); p != nil {
					s.setLiveProvider(requestID, p.Name, s.mgr.UpstreamModel(meta.Model, p.Name))
				}
				s.setLiveStatus(requestID, "connecting")
				res := s.exec.RunWithLease(ctx, meta.Model, upReq, lease, attempts)
				attempts += res.Attempts
				out := &queue.Result{
					Attempt: res.Attempts, RequeueAt: res.RequeueAt, Provider: res.Provider,
					UpstreamRequestID: res.UpstreamRequestID, ResponseModel: res.ResponseModel, CachedTokens: res.CachedTokens,
					CachedWriteTokens: res.CachedWriteTokens,
					UpstreamHeaderMs:  res.UpstreamHeaderMs, UpstreamContentType: res.UpstreamContentType,
				}
				if res.Err != nil {
					if !res.RequeueAt.IsZero() {
						s.log.Info("upstream retry scheduled", "request_id", requestID, "attempt", attempts,
							"provider", res.Err.Provider, "upstream_status", res.Err.UpstreamHTTP,
							"not_before", res.RequeueAt, "credential_retry_at", res.Err.RetryAt)
					}
					out.Err = res.Err
					return out
				}
				if err := compat.ApplyResponse(bridge, res.Resp); err != nil {
					out.Err = normalize.New(normalize.ErrUpstream, err.Error())
					return out
				}
				out.Status = res.Resp.StatusCode
				out.Header = res.Resp.Header
				out.Usage = queue.Usage{
					Prompt:     res.Resp.Usage.PromptTokens,
					Completion: res.Resp.Usage.CompletionTokens,
					Total:      res.Resp.Usage.TotalTokens,
				}
				out.UsageKnown = res.Resp.UsageKnown
				if res.Resp.Stream {
					out.Stream = res.Resp.StreamBody
				} else {
					out.Body = res.Resp.Body
				}
				return out
			},
		}

		// 8. 入队（资源足够时立即出队执行；不足则等待）
		queued, pos, err := s.sched.Enqueue(job, []limits.Gate{lease})
		if err != nil {
			s.record(r, k, requestID, meta.Model, meta.Stream, "error", "queue_full", start, 0, 0, 0)
			s.writeError(w, requestID, err.(*normalize.Error))
			return
		}
		handedOff.Store(true)
		if queued && pos > 0 && s.exposeQueuePos.Load() {
			w.Header().Set("X-Queue-Position", strconv.Itoa(pos))
		}
		s.log.Debug("enqueued", "request_id", requestID, "queued", queued, "pos", pos, "stream", meta.Stream)

		// 9. 挂起原 HTTP 请求等待结果（SSE 发送心跳）
		s.awaitResult(w, r, job, requestID, meta.Model, meta.Stream, k, start)
	}
}

// awaitResult 挂起等待 Job 结果；流式请求期间发送 SSE 心跳。
func (s *Server) awaitResult(w http.ResponseWriter, r *http.Request, job *queue.Job, requestID, model string, stream bool, k *key.Key, start time.Time) {
	if stream {
		s.awaitStream(w, r, job, requestID, model, k, start)
		return
	}

	select {
	case res := <-job.ResultCh():
		s.stats.RecordRequest(res.Queued, res.QueueWait(), res.Err == nil,
			res.Usage.Prompt, res.Usage.Completion, res.Usage.Total)
		s.recordResult(r, k, requestID, model, false, res, start)
		if res.Err != nil {
			s.writeError(w, requestID, res.Err)
			return
		}
		w.Header().Set("X-Harbor-Queue-Wait-Ms", strconv.FormatInt(res.QueueWait().Milliseconds(), 10))
		w.Header().Set("Content-Type", "application/json")
		s.setLiveStatus(requestID, "responding")
		w.WriteHeader(res.Status)
		_, _ = w.Write(res.Body)
	case <-r.Context().Done():
		s.sched.Cancel(job.ID)
		s.discardJobResult(job)
		s.record(r, k, requestID, model, false, "cancelled", "client_disconnected", start, 0, 0, 0)
	}
}

// Cancellation may win the select just as an upstream stream becomes ready.
// Drain that sole result so its stream and lease cannot be abandoned.
func (s *Server) discardJobResult(job *queue.Job) {
	go func() {
		res := <-job.ResultCh()
		if res.Stream != nil {
			_ = res.Stream.Close()
			s.applyStreamUsage(job, res)
			s.sched.ReleaseJob(job)
		}
	}()
}

// awaitStream 流式请求：排队期间按显式开启的 keepalive 发 SSE 心跳，
// 结果到达后逐帧转发。在第一个心跳或成功结果之前不提交 SSE 头：
// 上游很快失败时改走 JSON 502，避免客户端已按 SSE 解析后把 error 对象插成 [object Object]。
func (s *Server) awaitStream(w http.ResponseWriter, r *http.Request, job *queue.Job, requestID, model string, k *key.Key, start time.Time) {
	rc := http.NewResponseController(w)

	// 排队心跳：首帧延迟到「早退阈值 + 1s」之后。
	// 早退阈值之前不提交 SSE，排队失败（提前 429）才能保持干净的 HTTP 状态码；
	// 阈值之后才用注释帧保活，避免长排队被代理空闲超时掐断。
	ka := s.keepaliveConfig()
	var hb *time.Timer
	var hbC <-chan time.Time
	if ka.IsEnabled() {
		firstDelay := ka.IntervalDuration()
		if early := s.cfg.Queue.Early429AfterDuration(); early > 0 && early+time.Second > firstDelay {
			firstDelay = early + time.Second
		}
		hb = time.NewTimer(firstDelay)
		hbC = hb.C
		defer hb.Stop()
	}

	sseStarted := false
	startSSE := func() {
		if sseStarted {
			return
		}
		sseStarted = true
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		// 禁止中间代理对 SSE 再压缩或转换，避免逐块 Flush 后仍被攒成整段。
		w.Header().Set("Cache-Control", "no-cache, no-transform")
		w.Header().Set("X-Accel-Buffering", "no")
		// 不设 Connection：HTTP/2 禁止连接级头，且 SSE 靠 Content-Type 与分块传输保活。
		w.WriteHeader(http.StatusOK)
		_ = rc.Flush()
	}
	fail := func(res *queue.Result) {
		s.stats.RecordRequest(res.Queued, res.QueueWait(), false,
			res.Usage.Prompt, res.Usage.Completion, res.Usage.Total)
		s.recordResult(r, k, requestID, model, true, res, start)
		if !sseStarted {
			s.writeError(w, requestID, res.Err)
			return
		}
		s.log.Warn("request failed", "request_id", requestID, "code", res.Err.Code, "reason", res.Err.Reason)
		if r.URL.Path == "/v1/responses" {
			// SUB2API consumes Responses events, not Chat error data. Once a
			// heartbeat commits HTTP 200, a typed failure must end the stream.
			body, _ := json.Marshal(map[string]any{
				"type": "response.failed", "sequence_number": 0,
				"response": map[string]any{
					"id": "resp_" + requestID, "object": "response", "created_at": start.Unix(),
					"status": "failed", "model": model, "output": []any{}, "usage": nil,
					"error": map[string]string{"code": res.Err.Code, "message": res.Err.Message},
				},
			})
			_, _ = fmt.Fprintf(w, "event: response.failed\ndata: %s\n\n", body)
		} else {
			// Chat clients expect ordinary data with a string message.
			_, _ = fmt.Fprintf(w, "data: %s\n\n", normalize.StreamDataWithRequestID(res.Err, requestID))
		}
		_ = rc.Flush()
	}

	for {
		select {
		case res := <-job.ResultCh():
			if res.Err != nil {
				fail(res)
				return
			}
			startSSE()
			s.forwardStream(w, r, rc, job, res, requestID, model, k, start)
			return
		case <-hbC:
			startSSE()
			_, _ = io.WriteString(w, ka.Frame())
			_ = rc.Flush()
			hb.Reset(ka.IntervalDuration())
		case <-r.Context().Done():
			s.sched.Cancel(job.ID)
			s.discardJobResult(job)
			s.record(r, k, requestID, model, true, "cancelled", "client_disconnected", start, 0, 0, 0)
			return
		}
	}
}

// forwardStream 在一段已获得所有权的上游流上完成转发、用量结算与审计。
// 任何退出路径都关闭上游 Body 并释放并发槽，绝不把连接/槽位留在后台。
func (s *Server) forwardStream(w http.ResponseWriter, r *http.Request, rc *http.ResponseController, job *queue.Job, res *queue.Result, requestID, model string, k *key.Key, start time.Time) {
	defer func() {
		if res.Stream != nil {
			_ = res.Stream.Close()
		}
		s.sched.ReleaseJob(job)
	}()

	var streamErr error
	if res.Stream != nil {
		frames, stopEarly := compat.TerminalFrames(r.URL.Path)
		streamErr = s.copyStream(r.Context(), w, res.Stream, rc, func() {
			res.FirstTokenMs = time.Since(start).Milliseconds()
			s.setLiveStatus(requestID, "responding")
		}, terminalFrames{frames: frames, stopEarly: stopEarly})
		s.applyStreamUsage(job, res)
	} else {
		if _, err := w.Write(res.Body); err != nil {
			res.Err = normalize.New(normalize.ErrUpstream, "response write failed")
		}
		_ = rc.Flush()
	}

	// 结束判定只看 copyStream 的结果：正常结束（EOF 或已写出 [DONE]）即成功。
	// 不能事后再查 r.Context()——客户端收到 [DONE] 后通常立即断开，
	// 那时上下文已取消，但这次请求是完整交付的。
	clientGone := false
	switch {
	case streamErr == nil:
	case errors.Is(streamErr, errStreamIdle):
		res.Err = normalize.New(normalize.ErrTimeout, "upstream stream idle timeout")
		s.log.Warn("stream idle timeout", "request_id", requestID, "model", model)
	case errors.Is(streamErr, errDownstreamWrite) || r.Context().Err() != nil:
		// 客户端在流结束前断开：copyStream 已关闭上游 Body，槽位由 defer 释放。
		clientGone = true
	default:
		res.Err = normalize.New(normalize.ErrUpstream, "upstream stream failed or was interrupted")
		s.log.Warn("stream failed", "request_id", requestID, "code", res.Err.Code, "err", streamErr.Error())
	}

	s.stats.RecordRequest(res.Queued, res.QueueWait(), res.Err == nil && !clientGone,
		res.Usage.Prompt, res.Usage.Completion, res.Usage.Total)
	if clientGone {
		s.record(r, k, requestID, model, true, "cancelled", "client_disconnected", start,
			res.Usage.Prompt, res.Usage.Completion, res.Usage.Total)
		return
	}
	s.recordResult(r, k, requestID, model, true, res, start)
}

// errStreamIdle 表示上游在空闲超时内没有任何新数据。
var errStreamIdle = errors.New("upstream stream idle timeout")

// errDownstreamWrite 表示向客户端写数据失败（客户端已断开）。
var errDownstreamWrite = errors.New("downstream write failed")

// terminalFrames / terminalScan 对齐 agent2api `api::pipeline::TerminalScan`：
// 用滑动窗口（最长特征 - 1 字节）跨 chunk 匹配收尾帧，避免收尾帧被 TCP 分片
// 切开时漏判；`contains` 而不是 `HasSuffix`，因为一帧后面可能还跟着别的字节。
type terminalFrames struct {
	frames    []string
	stopEarly bool
}

type terminalScan struct {
	frames []string
	window []byte
	seen   bool
}

func newTerminalScan(tf terminalFrames) *terminalScan {
	if len(tf.frames) == 0 {
		return nil
	}
	return &terminalScan{frames: tf.frames}
}

func (s *terminalScan) push(chunk []byte) bool {
	if s == nil || s.seen {
		return false
	}
	s.window = append(s.window, chunk...)
	longest := 0
	for _, frame := range s.frames {
		if len(frame) > longest {
			longest = len(frame)
		}
		if bytes.Contains(s.window, []byte(frame)) {
			s.seen = true
		}
	}
	if keep := longest - 1; keep >= 0 && len(s.window) > keep {
		s.window = append(s.window[:0], s.window[len(s.window)-keep:]...)
	}
	return s.seen
}

// sseDoneMarkers 是 OpenAI 兼容流的终止事件（含 CRLF 变体）。
var sseDoneMarkers = []string{
	"data: [DONE]\n\n", "data:[DONE]\n\n",
	"data: [DONE]\r\n\r\n", "data:[DONE]\r\n\r\n",
}

// sseDoneLines 是只写出了 [DONE] 行、尚缺结尾空行的状态 → 需要补写的结尾。
// 上游或按行处理的管道可能把 "data: [DONE]\n" 与空行拆成两次写出；
// 客户端通常读到这一行就断开，因此这一行写出即视为终止事件已交付。
var sseDoneLines = []struct{ line, terminator string }{
	{"data: [DONE]\r\n", "\r\n"}, {"data:[DONE]\r\n", "\r\n"},
	{"data: [DONE]\n", "\n"}, {"data:[DONE]\n", "\n"},
}

// copyStream 逐块转发 SSE 字节：
//   - 已向客户端写出完整 `data: [DONE]` 事件即视为正常结束，立刻关闭上游，
//     不再等待上游 EOF（部分上游 [DONE] 后不立即断开，客户端也常在此时关闭连接）；
//   - 每收到一块上游数据即重置空闲读计时器；空闲到点或客户端断开时
//     立即关闭上游 Body，让阻塞中的 Read 返回并释放并发槽；
//   - 心跳仅在显式开启时才发送，且必须落在完整 SSE 事件边界（\n\n），
//     避免注释帧被拼进半行 data 造成 JSON 截断。
func (s *Server) copyStream(ctx context.Context, w io.Writer, src io.ReadCloser, rc *http.ResponseController, onFirst func(), terminals ...terminalFrames) error {
	ka := s.keepaliveConfig()
	keepalive := ka.IsEnabled()
	interval := ka.IntervalDuration()
	frame := ka.Frame()
	idle := time.Duration(s.streamIdleNs.Load())
	var scan *terminalScan
	stopEarly := false
	if len(terminals) > 0 {
		scan = newTerminalScan(terminals[0])
		stopEarly = terminals[0].stopEarly
	}

	// chunks 必须无缓冲：保证「先投递数据块、再投递 EOF」的顺序，
	// 否则两个通道同时就绪时 select 可能先取走 EOF 而丢掉最后一块数据。
	chunks := make(chan []byte)
	errs := make(chan error, 1)
	done := make(chan struct{})
	var closeOnce sync.Once
	closeSrc := func() { closeOnce.Do(func() { _ = src.Close() }) }
	defer close(done)
	defer closeSrc()

	go func() {
		br := bufio.NewReaderSize(src, 32<<10)
		for {
			buf := make([]byte, 4096)
			n, err := br.Read(buf)
			if n > 0 {
				select {
				case chunks <- buf[:n]:
				case <-done:
					return
				}
			}
			if err != nil {
				select {
				case errs <- err:
				case <-done:
				}
				return
			}
		}
	}()

	// tail 记录已写出的最后若干字节：用于判断 SSE 事件边界（心跳插入点）
	// 以及是否已写出终止事件 [DONE]。
	const tailSize = 24
	tail := make([]byte, 0, tailSize)
	appendTail := func(b []byte) {
		if len(b) >= tailSize {
			tail = append(tail[:0], b[len(b)-tailSize:]...)
			return
		}
		tail = append(tail, b...)
		if over := len(tail) - tailSize; over > 0 {
			tail = append(tail[:0], tail[over:]...)
		}
	}
	atEventBoundary := func() bool {
		n := len(tail)
		if n == 0 {
			return true
		}
		if n >= 2 && tail[n-1] == '\n' && tail[n-2] == '\n' {
			return true
		}
		if n >= 4 && tail[n-4] == '\r' && tail[n-3] == '\n' && tail[n-2] == '\r' && tail[n-1] == '\n' {
			return true
		}
		return false
	}
	doneWritten := func() bool {
		for _, marker := range sseDoneMarkers {
			if bytes.HasSuffix(tail, []byte(marker)) {
				return true
			}
		}
		return false
	}
	// doneLineWritten 返回 [DONE] 行已写出但缺少结尾空行时应补写的内容。
	doneLineWritten := func() (string, bool) {
		if doneWritten() {
			return "", false
		}
		for _, d := range sseDoneLines {
			if bytes.HasSuffix(tail, []byte(d.line)) {
				return d.terminator, true
			}
		}
		return "", false
	}
	boundaryRetry := 200 * time.Millisecond
	if interval < boundaryRetry {
		boundaryRetry = interval / 2
	}
	if boundaryRetry <= 0 {
		boundaryRetry = 10 * time.Millisecond
	}

	var kaTimer *time.Timer
	var kaC <-chan time.Time
	if keepalive {
		kaTimer = time.NewTimer(interval)
		kaC = kaTimer.C
		defer kaTimer.Stop()
	}
	var idleTimer *time.Timer
	var idleC <-chan time.Time
	if idle > 0 {
		idleTimer = time.NewTimer(idle)
		idleC = idleTimer.C
		defer idleTimer.Stop()
	}

	for {
		select {
		case b := <-chunks:
			if onFirst != nil {
				onFirst()
				onFirst = nil
			}
			if _, err := w.Write(b); err != nil {
				closeSrc()
				return fmt.Errorf("%w: %v", errDownstreamWrite, err)
			}
			appendTail(b)
			_ = rc.Flush()
			scan.push(b)
			if doneWritten() {
				// 终止事件已交付：结束转发并释放上游连接与并发槽。
				return nil
			}
			if terminator, ok := doneLineWritten(); ok {
				// [DONE] 行已交付：自行补齐结尾空行，不再等待上游的后续字节。
				// 客户端此时可能已断开，补写失败不影响「已完整交付」的判定。
				_, _ = io.WriteString(w, terminator)
				_ = rc.Flush()
				return nil
			}
			if stopEarly && scan != nil && scan.seen {
				// Chat 的 data: [DONE] 行就是完整事件；Responses/Anthropic
				// 的收尾是两行，提前停会截断 data 行，故只对 Chat 生效。
				return nil
			}
			if idleTimer != nil {
				resetIdleTimer(idleTimer, idle)
			}
			if kaTimer != nil {
				resetIdleTimer(kaTimer, interval)
			}
		case err := <-errs:
			if err == io.EOF {
				return nil
			}
			return err
		case <-idleC:
			closeSrc()
			return errStreamIdle
		case <-kaC:
			if !atEventBoundary() {
				// 上游正处在一个事件中间：先等它写完这条事件再补心跳。
				kaTimer.Reset(boundaryRetry)
				continue
			}
			if _, err := io.WriteString(w, frame); err != nil {
				closeSrc()
				return fmt.Errorf("%w: %v", errDownstreamWrite, err)
			}
			_ = rc.Flush()
			kaTimer.Reset(interval)
		case <-ctx.Done():
			// 已交付本协议收尾帧（或 Chat 的 [DONE] 行），或与上游 EOF 同时就绪时，
			// 认定为正常结束：EOF 晚于最后一块数据投递，说明客户端已收到全部内容。
			if (scan != nil && scan.seen) || doneWritten() {
				closeSrc()
				return nil
			}
			if _, ok := doneLineWritten(); ok {
				closeSrc()
				return nil
			}
			select {
			case err := <-errs:
				if err == io.EOF {
					return nil
				}
			default:
			}
			closeSrc()
			return ctx.Err()
		}
	}
}

func resetIdleTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

// keepaliveConfig 返回当前心跳配置快照。
func (s *Server) keepaliveConfig() config.KeepaliveConfig {
	s.kaMu.RLock()
	defer s.kaMu.RUnlock()
	return s.ka
}

// setRuntimeLimits 热更新请求总超时与排队位置头开关。
func (s *Server) setRuntimeLimits(requestTimeout time.Duration, expose bool) {
	if requestTimeout > 0 {
		s.requestTimeoutNs.Store(int64(requestTimeout))
	}
	s.exposeQueuePos.Store(expose)
}

// SetKeepalive 热更新心跳配置（管理台保存时调用）。
func (s *Server) SetKeepalive(ka config.KeepaliveConfig) {
	s.kaMu.Lock()
	s.ka = ka
	s.kaMu.Unlock()
}

func (s *Server) applyStreamUsage(job *queue.Job, res *queue.Result) {
	if mr, ok := res.Stream.(interface{ StreamModel() string }); ok {
		if m := mr.StreamModel(); m != "" {
			res.ResponseModel = m
		}
	}
	reader, ok := res.Stream.(provider.UsageReader)
	if !ok {
		return
	}
	usage, known := reader.StreamUsage()
	if !known {
		return
	}
	res.Usage = queue.Usage{
		Prompt:     usage.PromptTokens,
		Completion: usage.CompletionTokens,
		Total:      usage.TotalTokens,
	}
	res.UsageKnown = true
	res.CachedTokens = usage.CachedTokens
	res.CachedWriteTokens = usage.CachedWriteTokens
	job.SetTokenUsage(usage.TotalTokens, true)
}

// withStreamFlag 把顶层 stream 字段设为指定值（用于兼容没写 stream 的客户端）。
func withStreamFlag(body []byte, on bool) ([]byte, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return body, err
	}
	if raw == nil {
		return body, fmt.Errorf("body is not a JSON object")
	}
	if on {
		raw["stream"] = json.RawMessage("true")
	} else {
		delete(raw, "stream")
	}
	return json.Marshal(raw)
}

// handleModels 返回当前 Key 可用的统一模型名（聚合 routes，过滤掉仅作为映射目标的上游名）。
func (s *Server) handleModels(w http.ResponseWriter, r *http.Request) {
	k, err := s.keys.Authenticate(r.Header.Get("Authorization"))
	if err != nil {
		s.writeAuthError(w, s.newRequestID(), err)
		return
	}
	models := s.mgr.PublicModels()
	now := time.Now().Unix()
	data := make([]map[string]any, 0, len(models))
	for _, m := range models {
		// Key 白名单之外的模型不出现在列表里（调用时也会被 model_forbidden 拦下）。
		if !k.ModelAllowed(m) {
			continue
		}
		entry := map[string]any{
			"id":       m,
			"object":   "model",
			"created":  now,
			"owned_by": "harbor",
		}
		// 元数据：上下文窗口与能力（来自管理台声明 / models.dev 补齐）。
		if spec, ok := s.mgr.ModelSpec(m); ok {
			if spec.Context > 0 {
				entry["context_window"] = spec.Context
				entry["context_length"] = spec.Context
			}
			if len(spec.Capabilities) > 0 {
				entry["capabilities"] = spec.Capabilities
			}
		}
		data = append(data, entry)
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": data})
}

// --- Gate 管理 ---

type providerRouteGate struct {
	server    *Server
	model     string
	providers []*provider.Provider

	mu           sync.Mutex
	jobID        string
	reserve      int
	attemptSeq   uint64
	current      *provider.Provider
	currentID    string
	currentCost  float64
	currentGates []limits.Gate
}

func (g *providerRouteGate) Name() string { return "route:" + g.model }

func (g *providerRouteGate) TryAcquire() bool {
	return g.TryAcquireTokens(g.jobID, 0)
}

func (g *providerRouteGate) TryAcquireTokens(jobID string, n int) bool {
	if n < 0 {
		n = 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.current != nil {
		return false
	}
	g.jobID = jobID
	g.reserve = n
	for _, p := range g.providers {
		if !p.Available() {
			continue
		}
		gates := g.server.providerGates(p)
		attemptID := fmt.Sprintf("%s:attempt:%d", jobID, g.attemptSeq)
		g.attemptSeq++
		cost := g.effectiveCost(p)
		if tryAcquireProviderGates(gates, attemptID, n, cost) {
			g.current = p
			g.currentID = attemptID
			g.currentCost = cost
			g.currentGates = gates
			return true
		}
	}
	return false
}

// effectiveCost 返回该模型在指定 Provider 上实际占用的槽数。
// 系数来自 Provider 模型声明（模型管理页）；单个请求最多占满该 Provider 的
// 并发上限，避免系数大于上限时永远排不到。
func (g *providerRouteGate) effectiveCost(p *provider.Provider) float64 {
	cost := 1.0
	if g.server != nil && g.server.mgr != nil && p != nil {
		if declared := g.server.mgr.ModelConcurrencyCost(g.model, p.Name); declared > 0 {
			cost = declared
		}
	}
	if p != nil {
		if limit := p.Snapshot().Concurrency; limit > 0 && cost > float64(limit) {
			cost = float64(limit)
		}
	}
	return cost
}

func (g *providerRouteGate) SettleTokens(jobID string, actual int) {
	if actual < 0 {
		actual = 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.current == nil || g.jobID != jobID {
		return
	}
	g.releaseCurrentLocked(actual)
}

func (g *providerRouteGate) Release() {
	g.mu.Lock()
	jobID, reserve := g.jobID, g.reserve
	g.mu.Unlock()
	g.SettleTokens(jobID, reserve)
}

func (g *providerRouteGate) Provider() *provider.Provider {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.current
}

// 重试统一经过 Scheduler 重新排队，再获取完整 Provider Gate。

func (g *providerRouteGate) NextAvailable() (time.Time, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.current != nil {
		return time.Now(), true
	}
	now := time.Now()
	var next time.Time
	found := false
	for _, p := range g.providers {
		if !p.Available() {
			t := p.NextRecover()
			if t.After(now) && (!found || t.Before(next)) {
				next = t
				found = true
			}
			continue
		}
		for _, gate := range g.server.providerGates(p) {
			var t time.Time
			var ok bool
			if concurrency, isConcurrency := gate.(*limits.ConcurrencyGate); isConcurrency {
				t, ok = concurrency.NextAvailableCost(g.effectiveCost(p))
			} else {
				t, ok = gate.NextAvailable()
				if token, isToken := gate.(*limits.TokenGate); isToken {
					t, ok = token.NextAvailableTokens(g.reserve)
				}
			}
			if ok && t.After(now) && (!found || t.Before(next)) {
				next = t
				found = true
			}
		}
	}
	if !found {
		return time.Time{}, false
	}
	return next, true
}

func (g *providerRouteGate) releaseCurrentLocked(actual int) {
	releaseProviderGates(g.currentGates, g.currentID, actual, g.currentCost)
	g.current = nil
	g.currentID = ""
	g.currentCost = 0
	g.currentGates = nil
}

func tryAcquireProviderGates(gates []limits.Gate, jobID string, reserve int, cost float64) bool {
	type acquiredGate struct {
		gate   limits.Gate
		ticket *limits.RateTicket
	}
	acquired := make([]acquiredGate, 0, len(gates))
	for _, gate := range gates {
		var ok bool
		var ticket *limits.RateTicket
		if rate, isRate := gate.(*limits.RateGate); isRate {
			ticket, ok = rate.TryAcquireTicket()
		} else if concurrency, isConcurrency := gate.(*limits.ConcurrencyGate); isConcurrency {
			ok = concurrency.TryAcquireCost(cost)
		} else if ta, isTA := gate.(limits.TokenAcquirer); isTA {
			ok = ta.TryAcquireTokens(jobID, reserve)
		} else {
			ok = gate.TryAcquire()
		}
		if !ok {
			for i := len(acquired) - 1; i >= 0; i-- {
				if acquired[i].ticket != nil {
					acquired[i].ticket.Cancel()
				} else if concurrency, isConcurrency := acquired[i].gate.(*limits.ConcurrencyGate); isConcurrency {
					concurrency.ReleaseCost(cost)
				} else if ta, isTA := acquired[i].gate.(limits.TokenAcquirer); isTA {
					ta.SettleTokens(jobID, 0)
				} else {
					acquired[i].gate.Release()
				}
			}
			return false
		}
		acquired = append(acquired, acquiredGate{gate: gate, ticket: ticket})
	}
	return true
}

func releaseProviderGates(gates []limits.Gate, jobID string, actual int, cost float64) {
	for _, gate := range gates {
		if concurrency, isConcurrency := gate.(*limits.ConcurrencyGate); isConcurrency {
			concurrency.ReleaseCost(cost)
		} else if ta, isTA := gate.(limits.TokenAcquirer); isTA {
			ta.SettleTokens(jobID, actual)
		} else {
			gate.Release()
		}
	}
}

func (s *Server) newProviderLease(model string) *providerRouteGate {
	provs := s.mgr.ModelProviders(model)
	if len(provs) == 0 {
		return nil
	}
	return &providerRouteGate{server: s, model: model, providers: provs}
}

func (s *Server) providerGates(p *provider.Provider) []limits.Gate {
	s.gateMu.Lock()
	defer s.gateMu.Unlock()
	gs, ok := s.gateMap[p.Name]
	if !ok {
		gs = &gateSet{}
		s.gateMap[p.Name] = gs
	}
	view := p.Snapshot()
	// 额度更新作用于同一组 Gate，保留已有占用/窗口计数，避免新旧 Gate 并存导致绕过。
	if gs.concurrency == nil {
		gs.concurrency = limits.NewConcurrency("provider:"+view.Name+":concurrency", view.Concurrency)
		gs.rpm = limits.NewRate("provider:"+view.Name+":rpm", view.RPM, time.Minute)
		gs.token = limits.NewToken("provider:"+view.Name+":token", view.TPM, view.TPH, view.TPD)
	} else {
		gs.concurrency.UpdateLimit(view.Concurrency)
		gs.rpm.UpdateLimit(view.RPM)
		gs.token.UpdateLimits(view.TPM, view.TPH, view.TPD)
	}
	return []limits.Gate{gs.concurrency, gs.rpm, gs.token}
}

func (s *Server) gatesForProvider(model string) []limits.Gate {
	lease := s.newProviderLease(model)
	if lease == nil {
		return nil
	}
	return []limits.Gate{lease}
}

// --- 通用 ---

// estimatePromptTokens 粗略估计请求 token 数（TokenGate 预扣用）：
// 统计 messages 中文本内容的字符数 / 4，固定 +16 余量，并加上客户端声明的
// 最大输出 token 预算，避免只预扣 prompt 导致大输出请求集中穿透配额。
func estimatePromptTokens(body []byte) int {
	var m struct {
		Messages []struct {
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
		Input               json.RawMessage `json:"input"`
		Instructions        string          `json:"instructions"`
		MaxTokens           int64           `json:"max_tokens"`
		MaxCompletionTokens int64           `json:"max_completion_tokens"`
		MaxOutputTokens     int64           `json:"max_output_tokens"`
	}
	if err := json.Unmarshal(body, &m); err != nil {
		return len(body)/4 + 16
	}
	chars := 0
	if m.Instructions != "" {
		chars += len(m.Instructions)
	}
	if len(m.Input) > 0 && !bytes.Equal(bytes.TrimSpace(m.Input), []byte("null")) {
		chars += inputTextLen(m.Input)
	}
	for _, msg := range m.Messages {
		if len(msg.Content) == 0 {
			continue
		}
		var text string
		if msg.Content[0] == '"' {
			if json.Unmarshal(msg.Content, &text) == nil {
				chars += len(text)
			}
			continue
		}
		// 结构化 content（多模态）：只统计 text 部分。
		var parts []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if json.Unmarshal(msg.Content, &parts) == nil {
			for _, p := range parts {
				if p.Type == "text" {
					chars += len(p.Text)
				}
			}
		}
	}

	reserve := chars/4 + 16
	if chars == 0 {
		reserve = len(body)/4 + 16
	}
	maxOutput := m.MaxTokens
	if m.MaxCompletionTokens > maxOutput {
		maxOutput = m.MaxCompletionTokens
	}
	if m.MaxOutputTokens > maxOutput {
		maxOutput = m.MaxOutputTokens
	}
	if maxOutput > 0 {
		maxInt := int(^uint(0) >> 1)
		if maxOutput > int64(maxInt-reserve) {
			return maxInt
		}
		reserve += int(maxOutput)
	}
	return reserve
}

func inputTextLen(raw json.RawMessage) int {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return 0
	}
	if raw[0] == '"' {
		var s string
		if json.Unmarshal(raw, &s) == nil {
			return len(s)
		}
	}
	return len(raw)
}

// truncateUA 截断过长的 User-Agent，避免审计表膨胀。
func truncateUA(ua string) string {
	ua = strings.TrimSpace(ua)
	if len(ua) > 200 {
		return ua[:200]
	}
	return ua
}

// clientIP 取真实客户端地址（Fly / 反代场景优先 X-Forwarded-For 的第一跳）。
func clientIP(r *http.Request) string {
	if v := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); v != "" {
		if idx := strings.IndexByte(v, ','); idx >= 0 {
			v = v[:idx]
		}
		return strings.TrimSpace(v)
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

func (s *Server) newRequestID() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return "hr_" + hex.EncodeToString(b)
}

func (s *Server) writeAuthError(w http.ResponseWriter, requestID string, err error) {
	var he *normalize.Error
	switch {
	case strings.Contains(err.Error(), "banned"):
		he = normalize.ErrKeyBanned
	case strings.Contains(err.Error(), "expired"):
		he = normalize.ErrKeyExpired
	default:
		he = normalize.ErrInvalidAPIKey
	}
	s.writeError(w, requestID, normalize.New(he, err.Error()))
}

func (s *Server) writeError(w http.ResponseWriter, requestID string, he *normalize.Error) {
	s.log.Warn("request failed", "request_id", requestID, "code", he.Code, "reason", he.Reason)
	normalize.Write(w, he, requestID)
}

// isLiveStatus 判断请求是否仍在进行中。
func isLiveStatus(status string) bool {
	switch status {
	case "queued", "connecting", "waiting", "responding":
		return true
	default:
		return false
	}
}

// beginLiveRequest 在请求进入排队前创建一条进行中记录。
// 最终成功/失败时 record/recordResult 会用同 ID 覆盖它。
func (s *Server) beginLiveRequest(r *http.Request, k *key.Key, requestID, model string, stream bool, start time.Time, thinkingLevel string) {
	keyID := ""
	if k != nil {
		keyID = k.ID
	}
	log := &RequestLog{
		RequestID:     requestID,
		KeyID:         keyID,
		Model:         model,
		Status:        "queued",
		QueuedAt:      start,
		Stream:        stream,
		ClientUA:      truncateUA(r.UserAgent()),
		ClientIP:      clientIP(r),
		InboundPath:   r.URL.Path,
		ThinkingLevel: thinkingLevel,
	}
	s.pushRecent(log)
}

// updateLive 对进行中的记录做原地更新；已结束的记录不再修改。
func (s *Server) updateLive(requestID string, fn func(*RequestLog)) {
	s.recentMu.Lock()
	if log, ok := s.recent[requestID]; ok && isLiveStatus(log.Status) {
		fn(log)
	}
	s.recentMu.Unlock()
}

// setLiveStatus 推进进行中请求的阶段。
func (s *Server) setLiveStatus(requestID, status string) {
	if !isLiveStatus(status) {
		return
	}
	now := time.Now()
	s.updateLive(requestID, func(log *RequestLog) {
		if log.StartedAt.IsZero() && status != "queued" {
			log.StartedAt = now
		}
		if status == "connecting" && log.Attempts == 0 {
			log.Attempts = 1
		}
		if status == "responding" && log.FirstTokenMs == 0 {
			log.FirstTokenMs = now.Sub(log.QueuedAt).Milliseconds()
		}
		log.Status = status
	})
}

// setLiveProvider 在进行中记录上补实际命中的 Provider / 上游模型。
func (s *Server) setLiveProvider(requestID, providerName, upstreamModel string) {
	s.updateLive(requestID, func(log *RequestLog) {
		if providerName != "" {
			log.Provider = providerName
		}
		if upstreamModel != "" {
			log.UpstreamModel = upstreamModel
		}
	})
}

// liveMeta 读取进行中记录的快照（用于最终记录继承 Provider/开始时间/思考等级）。
func (s *Server) liveMeta(requestID string) RequestLog {
	s.recentMu.Lock()
	defer s.recentMu.Unlock()
	if log, ok := s.recent[requestID]; ok {
		return *log
	}
	return RequestLog{}
}

// liveThinkingLevel 读取进行中记录已解析出的思考等级，供最终记录继承。
func (s *Server) liveThinkingLevel(requestID string) string {
	return s.liveMeta(requestID).ThinkingLevel
}

// record 记录未产生 queue.Result 的请求（鉴权/校验失败等）。
func (s *Server) record(r *http.Request, k *key.Key, requestID, model string, stream bool, status, errCode string, start time.Time, tin, tout, total int) {
	live := s.liveMeta(requestID)
	log := &RequestLog{
		RequestID:     requestID,
		KeyID:         k.ID,
		Model:         model,
		Provider:      live.Provider,
		Status:        status,
		ErrorCode:     errCode,
		QueuedAt:      start,
		StartedAt:     live.StartedAt,
		FinishedAt:    time.Now(),
		Attempts:      live.Attempts,
		TokensIn:      tin,
		TokensOut:     tout,
		Stream:        stream,
		ClientUA:      truncateUA(r.UserAgent()),
		ClientIP:      clientIP(r),
		UpstreamModel: live.UpstreamModel,
		InboundPath:   r.URL.Path,
		ThinkingLevel: live.ThinkingLevel,
	}
	s.pushRecent(log)
	s.persist(log)
}

// recordResult 记录一次完整请求。
func (s *Server) recordResult(r *http.Request, k *key.Key, requestID, model string, stream bool, res *queue.Result, start time.Time) {
	status, errCode := "success", ""
	if res.Err != nil {
		status, errCode = "error", res.Err.Code
	}
	providerName := res.Provider
	if providerName == "" && res.Err != nil {
		providerName = res.Err.Provider
	}
	log := &RequestLog{
		RequestID:           requestID,
		KeyID:               k.ID,
		Model:               model,
		Provider:            providerName,
		Status:              status,
		ErrorCode:           errCode,
		QueuedAt:            res.EnqueuedAt,
		StartedAt:           res.DequeuedAt,
		FinishedAt:          time.Now(),
		QueueWaitMs:         res.QueueWait().Milliseconds(),
		Attempts:            res.Attempt,
		TokensIn:            res.Usage.Prompt,
		TokensOut:           res.Usage.Completion,
		Stream:              stream,
		ClientUA:            truncateUA(r.UserAgent()),
		ClientIP:            clientIP(r),
		FirstTokenMs:        res.FirstTokenMs,
		ResponseModel:       res.ResponseModel,
		UpstreamRequestID:   res.UpstreamRequestID,
		CachedTokens:        res.CachedTokens,
		CachedWriteTokens:   res.CachedWriteTokens,
		InboundPath:         r.URL.Path,
		UpstreamHeaderMs:    res.UpstreamHeaderMs,
		UpstreamContentType: res.UpstreamContentType,
		ThinkingLevel:       s.liveThinkingLevel(requestID),
	}
	if providerName != "" {
		log.UpstreamModel = s.mgr.UpstreamModel(model, providerName)
	}
	if res.Err != nil {
		// 上游原始错误信息仅进后台审计，不对客户端输出。
		log.UpstreamCode = res.Err.UpstreamCode
		log.UpstreamType = res.Err.UpstreamType
		log.UpstreamHTTP = res.Err.UpstreamHTTP
		log.ErrorDetail = res.Err.Reason
	}
	s.pushRecent(log)
	s.persist(log)
}

// persist 将请求记录异步镜像到 SQLite（P2）。
// 纯内存模式（store 为 nil）下为空操作；写失败只记日志，不影响响应。
// 异步保证热路径零阻塞；单条 INSERT 在 WAL + busy_timeout 下很快完成。
func (s *Server) persist(log *RequestLog) {
	if s.store == nil {
		return
	}
	rec := &db.Request{
		RequestID:           log.RequestID,
		KeyID:               log.KeyID,
		Model:               log.Model,
		Provider:            log.Provider,
		Status:              log.Status,
		ErrorCode:           log.ErrorCode,
		UpstreamCode:        log.UpstreamCode,
		UpstreamType:        log.UpstreamType,
		UpstreamHTTP:        log.UpstreamHTTP,
		ErrorDetail:         log.ErrorDetail,
		QueuedAt:            log.QueuedAt,
		StartedAt:           log.StartedAt,
		FinishedAt:          log.FinishedAt,
		QueueWaitMs:         log.QueueWaitMs,
		Attempts:            log.Attempts,
		TokensIn:            log.TokensIn,
		TokensOut:           log.TokensOut,
		ClientUA:            log.ClientUA,
		ClientIP:            log.ClientIP,
		UpstreamModel:       log.UpstreamModel,
		FirstTokenMs:        log.FirstTokenMs,
		ResponseModel:       log.ResponseModel,
		UpstreamRequestID:   log.UpstreamRequestID,
		CachedTokens:        log.CachedTokens,
		CachedWriteTokens:   log.CachedWriteTokens,
		Stream:              log.Stream,
		InboundPath:         log.InboundPath,
		UpstreamHeaderMs:    log.UpstreamHeaderMs,
		UpstreamContentType: log.UpstreamContentType,
		ThinkingLevel:       log.ThinkingLevel,
	}
	go func() {
		if err := s.store.RecordRequest(rec); err != nil {
			s.log.Warn("persist request failed", "request_id", rec.RequestID, "err", err.Error())
		}
	}()
}

func (s *Server) pushRecent(l *RequestLog) {
	const maxRecent = 1000
	s.recentMu.Lock()
	defer s.recentMu.Unlock()
	if _, exists := s.recent[l.RequestID]; !exists {
		s.recentOrder = append(s.recentOrder, l.RequestID)
	}
	s.recent[l.RequestID] = l
	for len(s.recentOrder) > maxRecent {
		old := s.recentOrder[0]
		s.recentOrder = s.recentOrder[1:]
		delete(s.recent, old)
	}
}

func (s *Server) onFinished(job *queue.Job, res *queue.Result) {
	// 统计已由 awaitResult 记录；此处仅补内存请求日志兜底
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
