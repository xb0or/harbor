// Package queue 实现 Harbor 的核心：FIFO 队列 + 事件驱动 Scheduler。
//
// 设计（计划书 §04 / §06）：
//   - 排队发生在真正调用上游之前；
//   - FIFO 出队，队头阻塞时后续请求不越位；
//   - 出队条件：该请求关联的所有 Gate（并发 / RPM / Token）全部可 acquire；
//   - 资源释放（Release）或窗口滑出（NextAvailable）时通过事件唤醒调度，
//     不轮询 CPU；
//   - 排队超过 Deadline 的请求被移除并返回 429。
package queue

import (
	"context"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"harbor/internal/limits"
	"harbor/internal/normalize"
)

// Usage 是单请求的 token 用量（由执行层解析）。
type Usage struct {
	Prompt     int
	Completion int
	Total      int
}

// Result 是 Job 的最终执行结果。
type Result struct {
	Status int
	Header http.Header
	Body   []byte
	Stream io.ReadCloser // 流式响应体（服务端负责 Close）
	Usage  Usage
	// UsageKnown 为 true 时 Token 字段来自上游可信 usage；false 时结算保留预扣量。
	UsageKnown          bool
	Err                 *normalize.Error
	Attempt             int
	Provider            string        // 实际命中的上游 Provider（透传 executor.Result）
	RequeueAt           time.Time     // 仅无响应的可恢复失败使用；不投递中间结果
	FirstTokenMs        int64         // 从请求开始到首帧写出的毫秒数（流式）
	UpstreamRequestID   string        // 上游返回的 x-request-id
	ResponseModel       string        // 上游响应体回显的模型名
	CachedTokens        int           // 命中缓存的输入 token
	CachedWriteTokens   int           // 写入缓存的 token
	UpstreamHeaderMs    int64         // 发出上游请求到收到响应头的耗时（毫秒）
	UpstreamContentType string        // 上游响应头 Content-Type
	Wait                time.Duration // 累计排队时间，不含执行时间
	Queued              bool          // 是否真正等待过（包含重排）
	// 排队统计
	EnqueuedAt time.Time
	DequeuedAt time.Time
}

// QueueWait 返回实际累计等待时间，不含上游执行和流式传输。
func (r *Result) QueueWait() time.Duration { return r.Wait }

// Job 是一个待调度请求。
type Job struct {
	ID       string
	KeyHash  string
	Model    string
	Priority int // 预留：一期统一 FIFO
	// Do 在获得执行资格后调用；返回最终结果（含收敛错误）。
	Do func(ctx context.Context) *Result
	// Ctx 是客户端上下文；断开时取消执行。
	Ctx context.Context
	// Cancel 用于 Scheduler 停止时主动终止该 Job 的派生上下文。
	Cancel func()

	// TokenReserve 是入队时的预估 token 数（服务端设置，TokenGate 预扣用）。
	TokenReserve int
	// tokenActual/tokenUsageKnown 在 TokenGate 结算时使用；未知 usage 时保留预扣量，
	// 避免上游不返回 usage 导致配额被错误回补。
	tokenMu         sync.Mutex
	tokenActual     int
	tokenUsageKnown bool

	// ReleaseResources 在 Job 不再使用请求体等外部资源时调用。
	// Scheduler 保证该回调最多执行一次。
	ReleaseResources func()

	// --- 内部状态（调度器管理） ---
	EnqueuedAt time.Time
	Deadline   time.Time
	// SoftDeadline 是「排队早退」阈值：到点仍未调度就返回 429，
	// 避免客户端一直干等到自己的超时。零值表示未启用。
	SoftDeadline        time.Time
	deadlineFromContext bool
	gates               []limits.Gate
	resultCh            chan *Result
	state               int32     // 0=queued 1=running 2=done 3=cancelled
	Position            int       // 队内位置（1 起）
	queuedAt            time.Time // 本轮排队起点
	firstDequeued       time.Time
	wait                time.Duration
	queued              bool
	notBefore           time.Time
	attempts            int
	lastErr             *normalize.Error
	stopWatch           func() bool // context.AfterFunc，终结时解除

	releaseOnce  sync.Once // 仅最终释放；重排按 attempt 单独释放 Gate
	resourceOnce sync.Once // 外部资源释放只允许一次
}

// ResultCh 返回结果通道。
func (j *Job) ResultCh() <-chan *Result { return j.resultCh }

// SetTokenUsage 设置执行后的 token 用量。known=false 时结算逻辑会保留预扣量。
func (j *Job) SetTokenUsage(actual int, known bool) {
	if actual < 0 {
		actual = 0
	}
	j.tokenMu.Lock()
	j.tokenActual = actual
	j.tokenUsageKnown = known
	j.tokenMu.Unlock()
}

func (j *Job) tokenUsage() (int, bool) {
	j.tokenMu.Lock()
	defer j.tokenMu.Unlock()
	return j.tokenActual, j.tokenUsageKnown
}

func (s *Scheduler) releaseResources(job *Job) {
	if job == nil || job.ReleaseResources == nil {
		return
	}
	job.resourceOnce.Do(job.ReleaseResources)
}

// ReleaseJob 释放 Job 占用的全部 Gate（幂等；可重复调用）。
// 非流式请求由调度器在 finish 时自动释放；
// 流式请求由服务端在流完全转发结束后调用，确保并发槽覆盖流全程
// （计划书 §21 验收 12/13）。
// Token Gate 走结算路径（SettleTokens(jobID, actual)），其余 Gate 走 Release。
func (s *Scheduler) releaseAttempt(job *Job) {
	actual, known := job.tokenUsage()
	if !known {
		actual = job.TokenReserve
	}
	for _, g := range job.gates {
		if ta, ok := g.(limits.TokenAcquirer); ok {
			ta.SettleTokens(job.ID, actual)
		} else {
			g.Release()
		}
	}
}

func (s *Scheduler) ReleaseJob(job *Job) { s.complete(job, true) }

// complete releases either a held final lease or an already-released waiter.
// Resource and cancellation cleanup always run exactly once.
func (s *Scheduler) complete(job *Job, releaseGates bool) {
	job.releaseOnce.Do(func() {
		if releaseGates {
			s.releaseAttempt(job)
		}
		s.mu.Lock()
		delete(s.running, job.ID)
		job.state = 2
		stopWatch := job.stopWatch
		s.mu.Unlock()
		if stopWatch != nil {
			stopWatch()
		}
		if job.Cancel != nil {
			job.Cancel()
		}
		s.releaseResources(job)
		s.signal()
	})
}

// ErrQueueFull 队列已满（计划书 §04.3：直接失败条件之一）。
var ErrQueueFull = normalize.New(normalize.ErrQueueFull, "queue depth exceeded")

// ErrWaitTimeout 等待超过 max_queue_wait。
var ErrWaitTimeout = normalize.New(normalize.ErrRateLimited, "exceeded max queue wait")

// Scheduler 是队列调度器。
type Scheduler struct {
	mu        sync.Mutex
	jobs      []*Job
	byID      map[string]*Job
	depth     int
	maxDepth  atomic.Int64 // 队列容量（可热更新）
	maxWait   atomic.Int64 // 最长等待 ns（可热更新）
	earlyWait atomic.Int64 // 早退阈值 ns（0 = 关闭）

	notify chan struct{}
	wake   *time.Timer
	done   chan struct{}
	wg     sync.WaitGroup

	running map[string]*Job
	stopped bool

	// 统计回调（可选）
	OnQueued   func(job *Job, position int)
	OnDequeue  func(job *Job)
	OnFinished func(job *Job, res *Result)

	stopOnce sync.Once
	jobsWG   sync.WaitGroup
}

// NewScheduler 构造调度器。
func NewScheduler(maxDepth int, maxWait time.Duration) *Scheduler {
	s := &Scheduler{
		byID:    map[string]*Job{},
		notify:  make(chan struct{}, 1),
		done:    make(chan struct{}),
		running: map[string]*Job{},
	}
	s.maxDepth.Store(int64(maxDepth))
	s.maxWait.Store(int64(maxWait))
	return s
}

// SetEarlyWait 设置排队早退阈值（0 关闭）；只影响之后入队的请求。
func (s *Scheduler) SetEarlyWait(d time.Duration) {
	if d > 0 {
		s.earlyWait.Store(int64(d))
	} else {
		s.earlyWait.Store(0)
	}
}

// SetLimits 热更新队列容量与最长等待；已入队请求的 Deadline 不回溯修改。
func (s *Scheduler) SetLimits(maxDepth int, maxWait time.Duration) {
	if maxDepth > 0 {
		s.maxDepth.Store(int64(maxDepth))
	}
	if maxWait > 0 {
		s.maxWait.Store(int64(maxWait))
	}
}

// Start 启动调度循环。
func (s *Scheduler) Start() {
	s.wg.Add(1)
	go s.loop()
}

func (s *Scheduler) Stop() {
	s.stopOnce.Do(func() {
		close(s.done)
		s.mu.Lock()
		var queued []*Job
		for _, job := range s.jobs {
			if job.state == 0 {
				job.state = 3
				queued = append(queued, job)
			}
		}
		running := make([]*Job, 0, len(s.running))
		for _, job := range s.running {
			running = append(running, job)
		}
		s.jobs = nil
		s.byID = map[string]*Job{}
		s.running = map[string]*Job{}
		s.depth = 0
		s.stopped = true
		s.mu.Unlock()

		for _, job := range queued {
			res := &Result{
				Err: normalize.New(normalize.ErrRestarting, "server restarting: scheduler stopped"), Attempt: job.attempts,
				EnqueuedAt: job.EnqueuedAt, DequeuedAt: job.firstDequeued,
				Wait: job.wait + time.Since(job.queuedAt), Queued: job.queued,
			}
			s.complete(job, false)
			select {
			case job.resultCh <- res:
			default:
			}
		}
		for _, job := range running {
			if job.Cancel != nil {
				job.Cancel()
			}
		}
		s.wg.Wait()
		s.jobsWG.Wait()
	})
}

// Enqueue 入队。若队列已满返回 ErrQueueFull。
// 入队后立即尝试出队（资源足够则本请求可能直接被调度执行）。
// 返回：queued=入队时未获得执行资格（真正排队）；position=等待位置（1 起）。
func (s *Scheduler) Enqueue(job *Job, gates []limits.Gate) (queued bool, position int, err error) {
	if job == nil {
		return false, 0, normalize.New(normalize.ErrInternal, "nil job")
	}
	if job.Ctx == nil {
		job.Ctx = context.Background()
	}
	if job.Ctx != nil && job.Ctx.Err() != nil {
		job.resultCh = make(chan *Result, 1)
		job.state = 3
		job.resultCh <- &Result{Err: normalize.New(normalize.ErrInternal, "request cancelled"), EnqueuedAt: time.Now(), DequeuedAt: time.Now()}
		s.releaseResources(job)
		return false, 0, nil
	}
	job.gates = gates
	job.resultCh = make(chan *Result, 1)
	job.EnqueuedAt = time.Now()
	job.queuedAt = job.EnqueuedAt
	job.Deadline = job.EnqueuedAt.Add(time.Duration(s.maxWait.Load()))
	job.deadlineFromContext = false
	if job.Ctx != nil {
		if ctxDeadline, ok := job.Ctx.Deadline(); ok && ctxDeadline.Before(job.Deadline) {
			job.Deadline = ctxDeadline
			job.deadlineFromContext = true
		}
	}
	if early := time.Duration(s.earlyWait.Load()); early > 0 {
		if soft := job.EnqueuedAt.Add(early); soft.Before(job.Deadline) {
			job.SoftDeadline = soft
		}
	}

	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return false, 0, normalize.New(normalize.ErrRestarting, "server restarting: scheduler stopped")
	}
	if job.Ctx != nil && job.Ctx.Err() != nil {
		s.mu.Unlock()
		job.state = 3
		job.resultCh <- &Result{Err: normalize.New(normalize.ErrInternal, "request cancelled"), EnqueuedAt: job.EnqueuedAt, DequeuedAt: time.Now()}
		s.releaseResources(job)
		return false, 0, nil
	}
	if s.depth >= int(s.maxDepth.Load()) {
		s.mu.Unlock()
		return false, 0, ErrQueueFull
	}
	job.state = 0
	s.jobs = append(s.jobs, job)
	s.byID[job.ID] = job
	s.depth++
	job.Position = len(s.jobs)
	job.stopWatch = context.AfterFunc(job.Ctx, func() { s.Cancel(job.ID) })

	// 新请求入队是调度触发来源之一：同步尝试出队，
	// 资源充足时本请求会被立即调度执行（不入队等待）。
	timedOut := s.dispatchLocked()
	// dispatchLocked 执行期间可能修改了 job 的状态
	_, stillQueued := s.byID[job.ID]
	if stillQueued {
		job.queued = true
		job.Position = jobPositionLocked(s.jobs, job)
	}
	position = job.Position
	s.mu.Unlock()

	s.deliverTimedOut(timedOut)
	if s.OnQueued != nil {
		s.OnQueued(job, position)
	}
	if stillQueued {
		// 仍有等待者：保留 notify 语义（虽已同步处理，但通知无害）
	}
	s.signal()
	return stillQueued, position, nil
}

// jobPositionLocked 返回 job 在队列中的 1 基位置（不存在返回 0）。
func jobPositionLocked(jobs []*Job, target *Job) int {
	for i, j := range jobs {
		if j == target {
			return i + 1
		}
	}
	return 0
}

// Cancel 移除等待者或取消在途请求；在途槽位由执行/流真正退出后释放。
func (s *Scheduler) Cancel(jobID string) {
	s.mu.Lock()
	job := s.byID[jobID]
	if job == nil {
		job = s.running[jobID]
	}
	if job == nil {
		s.mu.Unlock()
		return
	}
	if job.state == 0 {
		s.removeLocked(job)
		job.wait += time.Since(job.queuedAt)
		job.state = 3
		err := normalize.New(normalize.ErrInternal, "request cancelled")
		if job.Ctx.Err() == context.DeadlineExceeded {
			err = normalize.New(normalize.ErrTimeout, "request deadline exceeded")
		}
		res := &Result{Err: err, Attempt: job.attempts, EnqueuedAt: job.EnqueuedAt, DequeuedAt: job.firstDequeued, Wait: job.wait, Queued: job.queued}
		s.mu.Unlock()
		s.complete(job, false)
		select {
		case job.resultCh <- res:
		default:
		}
		return
	}
	cancel := job.Cancel
	if job.state == 1 {
		job.state = 3
	}
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// Stats 返回队列快照（供管理接口）。
type Stats struct {
	Depth       int
	MaxDepth    int
	Running     int
	LongestWait time.Duration
}

func (s *Scheduler) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := Stats{Depth: s.depth, MaxDepth: int(s.maxDepth.Load()), Running: len(s.running)}
	now := time.Now()
	for _, j := range s.jobs {
		if j.state == 0 {
			if w := j.wait + now.Sub(j.queuedAt); w > st.LongestWait {
				st.LongestWait = w
			}
		}
	}
	return st
}

// Waiter 是排队请求的只读快照。
type Waiter struct {
	ID         string
	Model      string
	Position   int
	EnqueuedAt time.Time
	Wait       time.Duration
}

// Waiters 返回按位置排序的排队请求快照。
func (s *Scheduler) Waiters() []Waiter {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Waiter, 0, len(s.jobs))
	for _, j := range s.jobs {
		if j.state == 0 {
			out = append(out, Waiter{ID: j.ID, Model: j.Model, Position: j.Position, EnqueuedAt: j.EnqueuedAt, Wait: j.wait + time.Since(j.queuedAt)})
		}
	}
	return out
}

// --- 内部实现 ---

func (s *Scheduler) signal() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *Scheduler) loop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.done:
			return
		case <-s.notify:
			s.dispatch()
		case <-s.wakeC():
			s.dispatch()
		}
	}
}

// wakeC 返回当前定时器通道（nil 时永不触发）。
func (s *Scheduler) wakeC() <-chan time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wake == nil {
		return nil
	}
	return s.wake.C
}

// dispatch 调度循环入口：锁内出队 + 锁外投递超时结果。
func (s *Scheduler) dispatch() {
	s.mu.Lock()
	timedOut := s.dispatchLocked()
	s.mu.Unlock()
	s.deliverTimedOut(timedOut)
}

// deliverTimedOut 只处理已离开队列且未持有 Gate 的请求。
func (s *Scheduler) deliverTimedOut(timedOut []*Job) {
	for _, j := range timedOut {
		res := &Result{
			Err: s.waitError(j), Attempt: j.attempts,
			EnqueuedAt: j.EnqueuedAt, DequeuedAt: j.firstDequeued,
			Wait: j.wait, Queued: j.queued,
		}
		s.complete(j, false)
		if s.OnFinished != nil {
			s.OnFinished(j, res)
		}
		select {
		case j.resultCh <- res:
		default:
		}
	}
}

// dispatchLocked 尝试从队头连续出队可执行请求（FIFO）。
// 调用方必须持有 s.mu；返回排队超时的 job（锁外投递）。
func (s *Scheduler) dispatchLocked() []*Job {
	var timedOut []*Job
	// Retried jobs preserve their original deadlines even at the tail.
	// Expiry is independent of FIFO admission order.
	now := time.Now()
	for i := 0; i < len(s.jobs); {
		job := s.jobs[i]
		if jobExpired(job, now) {
			s.removeLocked(job)
			job.state = 2
			job.wait += now.Sub(job.queuedAt)
			timedOut = append(timedOut, job)
			continue
		}
		i++
	}
	for {
		if len(s.jobs) == 0 {
			break
		}
		head := s.jobs[0]
		if head.state != 0 {
			s.jobs[0] = nil
			s.jobs = s.jobs[1:]
			s.depth--
			delete(s.byID, head.ID)
			s.releaseResources(head)
			continue
		}
		now := time.Now()
		if jobExpired(head, now) {
			// 排队超时：按计划书 §04.3 → 429；
			// 若更早触发的是请求 context deadline，则返回 504。
			s.removeLocked(head)
			head.state = 2
			head.wait += now.Sub(head.queuedAt)
			timedOut = append(timedOut, head)
			continue
		}
		if now.Before(head.notBefore) {
			break
		}
		// 尝试获取该请求所需全部 Gate
		if s.tryAcquireLocked(head) {
			s.removeLocked(head)
			head.state = 1
			s.runLocked(head, now)
			continue
		}
		break // 队头阻塞，FIFO 不越位
	}
	s.armWakeLocked()
	return timedOut
}

// tryAcquireLocked 非阻塞获取全部 gate；失败回滚已获取的。
// Token Gate 按 job 预扣量占用（TryAcquireTokens），其余按普通语义。
func (s *Scheduler) tryAcquireLocked(job *Job) bool {
	acquired := 0
	for _, g := range job.gates {
		var ok bool
		if ta, isTA := g.(limits.TokenAcquirer); isTA {
			ok = ta.TryAcquireTokens(job.ID, job.TokenReserve)
		} else {
			ok = g.TryAcquire()
		}
		if !ok {
			for i := 0; i < acquired; i++ {
				if ta, isTA := job.gates[i].(limits.TokenAcquirer); isTA {
					ta.SettleTokens(job.ID, 0) // 回滚预扣
				} else {
					job.gates[i].Release()
				}
			}
			return false
		}
		acquired++
	}
	return true
}

// runLocked 记录真实出队时刻。重排前后累积等待，不把执行耗时算进去。
func (s *Scheduler) runLocked(job *Job, deq time.Time) {
	if job.firstDequeued.IsZero() {
		job.firstDequeued = deq
	}
	job.wait += deq.Sub(job.queuedAt)
	job.notBefore = time.Time{}
	job.SetTokenUsage(0, false)
	if s.OnDequeue != nil {
		s.OnDequeue(job)
	}
	s.running[job.ID] = job
	s.jobsWG.Add(1)
	go func() {
		defer s.jobsWG.Done()
		res := job.Do(job.Ctx)
		if res == nil {
			res = &Result{Err: normalize.ErrInternal}
		}
		s.finish(job, res)
	}()
}

// removeLocked 从队列移除（调用方持锁）。
func (s *Scheduler) removeLocked(job *Job) {
	for i, j := range s.jobs {
		if j != job {
			continue
		}
		copy(s.jobs[i:], s.jobs[i+1:])
		last := len(s.jobs) - 1
		s.jobs[last] = nil // 不让已移除的 Job/请求体被 backing array 保留
		s.jobs = s.jobs[:last]
		delete(s.byID, job.ID)
		s.depth--
		// 修正 Position
		for i := range s.jobs {
			s.jobs[i].Position = i + 1
		}
		return
	}
}

// finish 不向客户端投递中间失败。只有无业务响应且有剩余预算的
// attempt 才允许重排，原 ID、resultCh、Deadline、请求体始终保留。
func (s *Scheduler) finish(job *Job, res *Result) {
	job.SetTokenUsage(res.Usage.Total, res.UsageKnown)
	retry := res.Err != nil && !res.RequeueAt.IsZero() && res.Stream == nil && len(res.Body) == 0
	if retry {
		s.releaseAttempt(job) // 不释放请求体，也不消耗最终 releaseOnce
	}
	s.mu.Lock()
	job.attempts += res.Attempt
	if retry {
		job.lastErr = res.Err
		now := time.Now()
		if !s.stopped && job.state != 3 && job.Ctx.Err() == nil && !jobExpired(job, now) && s.depth < int(s.maxDepth.Load()) {
			delete(s.running, job.ID)
			job.state = 0
			job.queued = true
			job.queuedAt = now
			job.notBefore = res.RequeueAt
			s.jobs = append(s.jobs, job) // 重试进入尾部，不抢占已等待请求
			s.byID[job.ID] = job
			s.depth++
			job.Position = len(s.jobs)
			s.armWakeLocked()
			s.mu.Unlock()
			s.signal()
			return
		}
		if job.Ctx.Err() != nil {
			res.Err = normalize.As(job.Ctx.Err())
		} else if s.stopped {
			res.Err = normalize.New(normalize.ErrRestarting, "server restarting: scheduler stopped")
		} else if jobExpired(job, now) {
			res.Err = s.waitError(job)
		} else if s.depth >= int(s.maxDepth.Load()) {
			res.Err = ErrQueueFull
		}
	}
	res.RequeueAt = time.Time{}
	res.Attempt = job.attempts
	res.EnqueuedAt, res.DequeuedAt = job.EnqueuedAt, job.firstDequeued
	res.Wait, res.Queued = job.wait, job.queued
	// SSE keeps running until the server closes the stream and releases its lease.
	if res.Stream == nil {
		job.state = 2
	}
	s.mu.Unlock()
	if res.Stream == nil {
		s.complete(job, !retry)
	}
	// Call observers before publishing ownership of res to the HTTP handler.
	if s.OnFinished != nil {
		s.OnFinished(job, res)
	}
	select {
	case job.resultCh <- res:
	default:
	}
}

func (s *Scheduler) waitError(job *Job) *normalize.Error {
	if job.deadlineFromContext {
		return normalize.New(normalize.ErrTimeout, "request deadline exceeded")
	}
	now := time.Now()
	if !job.SoftDeadline.IsZero() && !now.Before(job.SoftDeadline) && now.Before(job.Deadline) {
		e := normalize.New(normalize.ErrRateLimited, "queue wait exceeded early threshold")
		e.Reason = "early 429: queued longer than early_429_after"
		return e
	}
	if job.lastErr != nil {
		err := normalize.Finalize([]*normalize.Error{job.lastErr})
		err.Reason += "; retry wait deadline exceeded"
		return err
	}
	return ErrWaitTimeout
}

// jobExpired 判断排队是否已到硬超时（Deadline）或早退阈值（SoftDeadline）。
func jobExpired(job *Job, now time.Time) bool {
	if !job.Deadline.IsZero() && !now.Before(job.Deadline) {
		return true
	}
	return !job.SoftDeadline.IsZero() && !now.Before(job.SoftDeadline)
}

// armWakeLocked 计算下次可调度时间：取队头 Gate 的未来唤醒时间与 Deadline 的较早者。
func (s *Scheduler) armWakeLocked() {
	if len(s.jobs) == 0 {
		if s.wake != nil {
			s.wake.Stop()
			s.wake = nil
		}
		return
	}
	head := s.jobs[0]
	now := time.Now()
	var next time.Time
	found := false
	for _, job := range s.jobs {
		if !job.Deadline.IsZero() && (!found || job.Deadline.Before(next)) {
			next, found = job.Deadline, true
		}
		if !job.SoftDeadline.IsZero() && (!found || job.SoftDeadline.Before(next)) {
			next, found = job.SoftDeadline, true
		}
	}
	if head.notBefore.After(now) && (!found || head.notBefore.Before(next)) {
		next, found = head.notBefore, true
	}
	for _, g := range head.gates {
		t, ok := g.NextAvailable()
		if token, isToken := g.(*limits.TokenGate); isToken {
			t, ok = token.NextAvailableTokens(head.TokenReserve)
		}
		// now 表示当前 Gate 没有未来的可预测唤醒点；
		// 不能把它当作零时长 Timer，否则另一个阻塞 Gate 会导致忙循环。
		if !ok || !t.After(now) {
			continue
		}
		if !found || t.Before(next) {
			next = t
			found = true
		}
	}
	var delay time.Duration
	if found {
		delay = time.Until(next)
		if delay < 0 {
			delay = 0
		}
	} else {
		delay = time.Hour // 不可预测：等 Release 事件
	}
	if s.wake != nil {
		s.wake.Stop()
	}
	s.wake = time.NewTimer(delay)
	// 丢弃旧 timer 触发（select 已取走旧 channel）
}
