// Package executor 负责把一次入队后的请求执行到最终结果：
// 在 routes 内按优先级选择可用 Provider/凭据，失败自动切换，
// 受 max_provider_attempts 与退避限制，绝不让上游出现无限重试。
package executor

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"time"

	"harbor/internal/normalize"
	"harbor/internal/provider"
)

// Executor 是上游执行器。
type Executor struct {
	mgr *provider.Manager

	mu       sync.RWMutex
	maxTries int
	backoff  time.Duration
	cooldown time.Duration
}

// New 构造执行器。
func New(mgr *provider.Manager, maxTries int, backoff, cooldown time.Duration) *Executor {
	if maxTries <= 0 {
		maxTries = 3
	}
	if backoff <= 0 {
		backoff = 500 * time.Millisecond
	}
	if cooldown <= 0 {
		cooldown = 30 * time.Second
	}
	return &Executor{mgr: mgr, maxTries: maxTries, backoff: backoff, cooldown: cooldown}
}

// Result 是一次执行的总结果。
type Result struct {
	Resp      *provider.Response // 成功时非空
	Attempts  int
	Provider  string           // 实际命中的上游 Provider（本轮最后尝试；成功或可恢复失败时非空）
	Err       *normalize.Error // 失败时非空（已收敛）
	RequeueAt time.Time        // 非零时尚有预算，由 Scheduler 等待并重新获取额度

	UpstreamRequestID string // 上游返回的 x-request-id
	ResponseModel     string // 上游响应体回显的模型名
	CachedTokens      int    // 命中缓存的输入 token
	CachedWriteTokens int    // 写入缓存的 token
}

// ProviderLease 是 Scheduler 已获取全部额度的 Provider 租约。
// 每次尝试结束后由 Scheduler 释放；重排后重新申请，不能持槽睡眠。
type ProviderLease interface {
	Provider() *provider.Provider
}

// maxTriesNow 返回当前重试上限（可热更新）。
func (e *Executor) maxTriesNow() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.maxTries
}

// baseBackoff 返回当前退避基数。
func (e *Executor) baseBackoff() time.Duration {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.backoff
}

// SetPolicy 热更新重试策略（管理台用）。
func (e *Executor) SetPolicy(maxTries int, backoff time.Duration) {
	if maxTries <= 0 {
		maxTries = 3
	}
	if backoff < 0 {
		backoff = 0
	}
	e.mu.Lock()
	e.maxTries = maxTries
	e.backoff = backoff
	e.mu.Unlock()
}

// Run 用于不经过 Scheduler 的内部调用，保留有界 failover。
func (e *Executor) Run(ctx context.Context, model string, req *provider.Request) *Result {
	providers := e.mgr.ModelProviders(model)
	if len(providers) == 0 {
		return &Result{Err: normalize.New(normalize.ErrModelForbidden, "no route configured for model "+model)}
	}
	var errs []*normalize.Error
	for attempt := 0; attempt < e.maxTriesNow(); attempt++ {
		if ctx.Err() != nil {
			return &Result{Attempts: attempt, Err: contextError(ctx)}
		}
		p := e.pickAvailable(providers)
		if p == nil {
			if len(errs) == 0 {
				errs = append(errs, normalize.Upstream("", 503, "all providers unavailable"))
			}
			return &Result{Attempts: attempt, Err: normalize.Finalize(errs)}
		}
		resp, err := e.execute(ctx, model, req, p)
		if err == nil {
			res := &Result{Resp: resp, Attempts: attempt + 1, Provider: p.Name}
			fillResultMeta(res, resp)
			return res
		}
		he := normalize.As(err)
		if he.Provider == "" {
			he.Provider = p.Name
		}
		errs = append(errs, he)
		if !he.Retryable && he.Class != normalize.ClassUpstreamKey {
			return &Result{Err: he, Attempts: attempt + 1, Provider: he.Provider}
		}
		if attempt+1 < e.maxTriesNow() {
			select {
			case <-ctx.Done():
				return &Result{Err: contextError(ctx), Attempts: attempt + 1}
			case <-time.After(e.backoffDuration(attempt)):
			}
		}
	}
	return &Result{Err: normalize.Finalize(errs), Attempts: e.maxTriesNow()}
}

// RunWithLease 只执行一次。completed 是同一请求已执行次数，跨重排不清零。
// HTTP 错误响应尚未输出业务数据时，返回延迟调度提示而非睡眠/直接报错。
func (e *Executor) RunWithLease(ctx context.Context, model string, req *provider.Request, lease ProviderLease, completed int) *Result {
	if ctx.Err() != nil {
		return &Result{Err: contextError(ctx)}
	}
	if completed >= e.maxTriesNow() {
		return &Result{Err: normalize.New(normalize.ErrUpstream, "provider attempt budget exhausted")}
	}
	p := lease.Provider()
	if p == nil {
		return &Result{Err: normalize.New(normalize.ErrInternal, "missing provider lease")}
	}
	resp, err := e.execute(ctx, model, req, p)
	res := &Result{Resp: resp, Attempts: 1, Provider: p.Name}
	if err == nil {
		fillResultMeta(res, resp)
		return res
	}
	he := normalize.As(err)
	res.Err = he
	if he.Provider == "" {
		he.Provider = p.Name
	}
	if ctx.Err() != nil {
		res.Err = contextError(ctx)
		return res
	}
	recoverable := he.Retryable
	if he.Class == normalize.ClassUpstreamKey {
		// A disabled credential will never recover by waiting. Only wait for
		// another active/cooling credential, not an all-disabled route.
		for _, candidate := range e.mgr.ModelProviders(model) {
			if candidate.Available() || candidate.NextRecover().After(time.Now()) {
				recoverable = true
				break
			}
		}
	}
	if recoverable && completed+1 < e.maxTriesNow() {
		// Credential Retry-After/cooldown is enforced by the route Gate.
		// This delay is per request; a healthy backup need not wait for the
		// failed credential's entire cooldown.
		res.RequeueAt = time.Now().Add(e.backoffDuration(completed))
	} else if he.Retryable || he.Class == normalize.ClassUpstreamKey {
		res.Err = normalize.Finalize([]*normalize.Error{he})
	}
	return res
}

// fillResultMeta 从上游响应中提取审计用的元信息（头 / 模型 / 缓存命中）。
func fillResultMeta(res *Result, resp *provider.Response) {
	if resp == nil {
		return
	}
	if resp.Header != nil {
		for _, key := range []string{"X-Request-Id", "X-Request-ID", "Request-Id", "X-Amzn-Requestid", "X-Trace-Id"} {
			if v := strings.TrimSpace(resp.Header.Get(key)); v != "" {
				res.UpstreamRequestID = v
				break
			}
		}
	}
	res.ResponseModel = resp.Model
	res.CachedTokens = resp.Usage.CachedTokens
	res.CachedWriteTokens = resp.Usage.CachedWriteTokens
}

func (e *Executor) execute(ctx context.Context, model string, req *provider.Request, p *provider.Provider) (*provider.Response, error) {
	upstreamModel := e.mgr.UpstreamModel(model, p.Name)
	if upstreamModel != model {
		copy := *req
		copy.Body = rewriteModel(req.Body, upstreamModel)
		req = &copy
	}
	return p.Execute(ctx, req)
}

func (e *Executor) backoffDuration(attempt int) time.Duration {
	const maxBackoff = time.Hour
	d := e.baseBackoff()
	for i := 0; i < attempt; i++ {
		if d >= maxBackoff/2 {
			return maxBackoff
		}
		d *= 2
	}
	if d > maxBackoff {
		return maxBackoff
	}
	return d
}

func contextError(ctx context.Context) *normalize.Error {
	if ctx.Err() == context.DeadlineExceeded {
		return normalize.New(normalize.ErrTimeout, "request deadline exceeded")
	}
	return normalize.New(normalize.ErrInternal, "request cancelled")
}

// pickAvailable 在 providers 中选出第一个当前可用的
// （按 routes 声明顺序，即优先级）。
func (e *Executor) pickAvailable(providers []*provider.Provider) *provider.Provider {
	// 冷却中的 provider 跳过；所有冷却则返回最早恢复的（由 Run 决定不再等）
	for _, p := range providers {
		if p.Available() {
			return p
		}
	}
	return nil
}

// RecoverTime 返回全部出口冷却时预计最早恢复时间（供调度器参考）。
func (e *Executor) RecoverTime(model string) time.Time {
	var times []time.Time
	for _, p := range e.mgr.ModelProviders(model) {
		times = append(times, p.NextRecover())
	}
	if len(times) == 0 {
		return time.Now()
	}
	sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
	return times[0]
}

// rewriteModel 将请求体 JSON 中的 "model" 字段替换为上游模型名。
// 使用 map[string]json.RawMessage 保留所有其他字段不变。
// 解析失败时返回原始 body（不阻断请求）。
func rewriteModel(body []byte, newModel string) []byte {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return body
	}
	mb, _ := json.Marshal(newModel)
	raw["model"] = mb
	out, err := json.Marshal(raw)
	if err != nil {
		return body
	}
	return out
}
