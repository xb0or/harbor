// Package normalize 负责 Harbor 的对外错误收敛：
// 所有上游 / 内部错误统一映射为稳定、简洁的 Harbor 错误格式，
// 原始错误只保留在内部日志，绝不透传给客户端。
package normalize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Error 是 Harbor 统一的内部错误类型（含对外映射信息）。
type Error struct {
	// HTTP 对外状态码
	Status int
	// Code 对外稳定错误码（见计划书 §08）
	Code string
	// Message 对外用户可读信息
	Message string
	// Type 对外错误类型
	Type string
	// Reason 内部日志用的详细原因（绝不外发）
	Reason string
	// Retryable 是否属于可恢复的瞬时错误（可切换/重排队）
	Retryable bool
	// RetryAt 最早重试时间（仅内部；对外只用于生成 Retry-After）。
	RetryAt time.Time `json:"-"`
	// Class 上游错误粗分类
	Class Class
	// UpstreamHTTP 上游原始状态码（仅内部）
	UpstreamHTTP int
	// Provider 出错的提供商名（仅内部）
	Provider string
	// UpstreamCode 上游原始结构化错误码（如 context_length_exceeded），仅内部审计用。
	UpstreamCode string
	// UpstreamType 上游原始错误类型（仅内部审计用）。
	UpstreamType string
	// UpstreamMessage 上游原始错误描述（截断后），仅内部审计用，绝不对外输出。
	UpstreamMessage string
}

// upstreamMessageMax 是上游原始错误描述保留的最大长度（仅内部审计）。

// Error 实现 error 接口（只暴露内部原因，供日志使用）。
func (e *Error) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("[%s] %s (status=%d) %s", e.Code, e.Message, e.Status, e.Reason)
	}
	return fmt.Sprintf("[%s] %s (status=%d)", e.Code, e.Message, e.Status)
}

// Class 是上游错误的粗分类，决定 Harbor 的处置策略。
type Class int

const (
	ClassUnknown Class = iota
	// ClassRateLimit 上游限流 / 配额不足（429 / 529 / insufficient_quota）
	ClassRateLimit
	// ClassUpstreamKey 上游凭证无效（401 / 403）
	ClassUpstreamKey
	// ClassTimeout 连接 / 响应超时（408 / timeout / dial error）
	ClassTimeout
	// ClassUpstream5xx 上游内部故障（500 / 502 / 503）
	ClassUpstream5xx
	// ClassBadRequest 请求本身非法（400），不重试
	ClassBadRequest
	// ClassModelNotFound 模型不存在（404），不重试
	ClassModelNotFound
)

// 对外固定错误格式（计划书 §08）
type wireError struct {
	Error wireErrorBody `json:"error"`
}

type wireErrorBody struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code"`
	// RequestID 供客户端/运维用它对后台定位到具体请求。
	RequestID string `json:"request_id,omitempty"`
}

// 错误常量（对外收敛结果，计划书 §08 表格）
var (
	ErrInvalidAPIKey  = &Error{Status: 401, Code: "invalid_api_key", Type: "invalid_request_error", Message: "Invalid API key"}
	ErrKeyExpired     = &Error{Status: 401, Code: "key_expired", Type: "invalid_request_error", Message: "API key expired"}
	ErrKeyBanned      = &Error{Status: 403, Code: "key_banned", Type: "permission_error", Message: "API key banned"}
	ErrModelForbidden = &Error{Status: 403, Code: "model_forbidden", Type: "permission_error", Message: "Model not allowed"}
	ErrInvalidRequest = &Error{Status: 400, Code: "invalid_request", Type: "invalid_request_error", Message: "Invalid request"}
	ErrRateLimited    = &Error{Status: 429, Code: "rate_limit_exceeded", Type: "rate_limit_error", Message: "Rate limited"}
	ErrQueueFull      = &Error{Status: 429, Code: "rate_limit_exceeded", Type: "rate_limit_error", Message: "Queue is full, try again later"}
	ErrUpstream       = &Error{Status: 502, Code: "upstream_error", Type: "upstream_error", Message: "Upstream service error"}
	ErrTimeout        = &Error{Status: 504, Code: "timeout", Type: "timeout_error", Message: "Request timed out"}
	ErrInternal       = &Error{Status: 500, Code: "internal_error", Type: "internal_error", Message: "Internal server error"}
	ErrRestarting     = &Error{Status: 503, Code: "server_restarting", Type: "server_error", Message: "Server is restarting, please retry"}
)

// New 构造带内部原因的收敛错误。
func New(base *Error, reason string) *Error {
	e := *base
	e.Reason = reason
	return &e
}

// Upstream 构造一个上游来源的错误。
func Upstream(pname string, status int, body string) *Error {
	cls, retryable := Classify(status, body)
	code, typ, msg := ParseUpstreamError(body)
	e := &Error{
		Class:           cls,
		Retryable:       retryable,
		UpstreamHTTP:    status,
		Provider:        pname,
		UpstreamCode:    code,
		UpstreamType:    typ,
		UpstreamMessage: truncate(msg, upstreamMessageMax),
	}
	switch cls {
	case ClassRateLimit:
		e.Status, e.Code, e.Type, e.Message = 429, "rate_limit_exceeded", "rate_limit_error", "Rate limited"
	case ClassUpstreamKey:
		e.Status, e.Code, e.Type, e.Message = 502, "upstream_error", "upstream_error", "Upstream service error"
	case ClassTimeout:
		e.Status, e.Code, e.Type, e.Message = 504, "timeout", "timeout_error", "Request timed out"
	case ClassBadRequest:
		e.Status, e.Code, e.Type, e.Message = 400, "invalid_request", "invalid_request_error", "Invalid request"
	case ClassModelNotFound, ClassUpstream5xx, ClassUnknown:
		e.Status, e.Code, e.Type, e.Message = 502, "upstream_error", "upstream_error", "Upstream service error"
	}
	// 只记录大小，不记录上游原始 Body，避免 Prompt/用户输入进入日志。
	e.Reason = fmt.Sprintf("provider=%s status=%d response_bytes=%d", pname, status, len(body))
	return e
}

// ParseUpstreamError 从 OpenAI 兼容的上游错误体提取结构化 code/type/message，
// 供内部审计使用（绝不对外输出）。解析失败返回空串。
func ParseUpstreamError(body string) (code, typ, msg string) {
	var upstream struct {
		Error struct {
			Message string          `json:"message"`
			Type    string          `json:"type"`
			Code    json.RawMessage `json:"code"`
		} `json:"error"`
	}
	if json.Unmarshal([]byte(body), &upstream) != nil {
		return "", "", ""
	}
	// code 可能是字符串、数字、null 或缺失：字符串直接取，数字按原样文本保留。
	if len(upstream.Error.Code) > 0 {
		var s string
		if json.Unmarshal(upstream.Error.Code, &s) == nil {
			code = s
		} else {
			code = string(upstream.Error.Code)
		}
	}
	return code, upstream.Error.Type, upstream.Error.Message
}

// truncate 截断字符串到 max 个 rune，超出部分省略。
func truncate(s string, max int) string {
	if max <= 0 {
		return ""
	}
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max]) + "…"
}

// upstreamMessageMax 为上游原始描述保留最大字符数（仅内部审计用）。
const upstreamMessageMax = 512

// Classify 把上游 HTTP 状态 + 响应体分类为粗粒度错误类型。
func Classify(status int, body string) (Class, bool) {
	// 只信任结构化错误码，不扫描 message/prompt 等可含用户输入的字段。
	var upstream struct {
		Error struct {
			Code json.RawMessage `json:"code"`
			Type json.RawMessage `json:"type"`
		} `json:"error"`
	}
	if json.Unmarshal([]byte(body), &upstream) == nil {
		rateLimited := false
		for _, raw := range []json.RawMessage{upstream.Error.Code, upstream.Error.Type} {
			var code string
			if json.Unmarshal(raw, &code) != nil {
				continue
			}
			switch strings.ToLower(strings.TrimSpace(code)) {
			case "insufficient_quota":
				// 硬配额不足不是等待几秒即可恢复的瞬时限流。
				return ClassRateLimit, false
			case "rate_limit", "rate_limit_error", "rate_limit_exceeded":
				rateLimited = true
			}
		}
		if rateLimited {
			return ClassRateLimit, true
		}
	}
	// 兼容简短纯文本错误；不能因整段 body 包含关键词就改变分类。
	switch strings.ToLower(strings.TrimSpace(body)) {
	case "rate_limit", "rate_limit_error", "rate_limit_exceeded", "rate limit", "rate limit exceeded":
		return ClassRateLimit, true
	}
	switch status {
	case 400:
		return ClassBadRequest, false
	case 401, 403:
		return ClassUpstreamKey, false
	case 404:
		return ClassModelNotFound, false
	case 408, 409:
		return ClassTimeout, true
	case 429:
		return ClassRateLimit, true
	case 500, 502, 503:
		return ClassUpstream5xx, true
	case 504:
		return ClassTimeout, true
	case 529:
		return ClassRateLimit, true
	default:
		// 未知错误保守处理：视为不可重试，避免无限重试
		return ClassUnknown, false
	}
}

// Finalize 把所有尝试都失败后的最终错误收敛为对外错误。
// 规则（计划书 §08）：
//   - 最后一次是限流类 → 429 rate_limit_exceeded（资源不足，无法继续等待）
//   - 超时类 → 504 timeout
//   - 其余上游故障 → 502 upstream_error
func Finalize(errs []*Error) *Error {
	if len(errs) == 0 {
		return ErrInternal
	}
	last := errs[len(errs)-1]
	base := ErrUpstream
	switch last.Class {
	case ClassRateLimit:
		base = ErrRateLimited
	case ClassTimeout:
		base = ErrTimeout
	}
	e := *last
	e.Status, e.Code, e.Type, e.Message = base.Status, base.Code, base.Type, base.Message
	e.Retryable = false
	return &e
}

// Body 返回对外 JSON 错误体：{"error":{"message":"...","type":"...","code":"..."}}。
// message 永远是 string，绝不把对象塞进去。
func Body(e *Error) []byte {
	return BodyWithRequestID(e, "")
}

// BodyWithRequestID 返回对外 JSON 错误体，并在 error 对象内携带 request_id，
// 方便客户端/运维持 ID 到后台定位具体请求。
func BodyWithRequestID(e *Error, requestID string) []byte {
	if e == nil {
		e = ErrInternal
	}
	body, err := json.Marshal(wireError{
		Error: wireErrorBody{
			Message:   e.Message,
			Type:      e.Type,
			Code:      e.Code,
			RequestID: requestID,
		},
	})
	if err != nil {
		return []byte(`{"error":{"message":"Internal server error","type":"internal_error","code":"internal_error"}}`)
	}
	return body
}

// StreamData 是 SSE data 行用的错误 JSON。
// 在稳定 error 对象之外再放顶层 message 字符串，避免部分客户端
// 把整个 error 对象模板插值成 [object Object]。
func StreamData(e *Error) []byte {
	return StreamDataWithRequestID(e, "")
}

// StreamDataWithRequestID 同 StreamData，并在 error 对象内携带 request_id。
func StreamDataWithRequestID(e *Error, requestID string) []byte {
	if e == nil {
		e = ErrInternal
	}
	body, err := json.Marshal(struct {
		Error   wireErrorBody `json:"error"`
		Message string        `json:"message"`
	}{
		Error: wireErrorBody{
			Message:   e.Message,
			Type:      e.Type,
			Code:      e.Code,
			RequestID: requestID,
		},
		Message: e.Message,
	})
	if err != nil {
		return Body(e)
	}
	return body
}

// Write 把收敛错误写入 http.ResponseWriter，绝不包含内部信息。
func Write(w http.ResponseWriter, e *Error, requestID string) {
	if e == nil {
		e = ErrInternal
	}
	h := w.Header()
	h.Set("Content-Type", "application/json")
	if requestID != "" {
		h.Set("X-Request-Id", requestID)
	}
	if e.Status == 429 || e.Status == 503 || e.Status == 504 {
		if delay := time.Until(e.RetryAt); delay > 0 {
			seconds := int64(delay / time.Second)
			if delay%time.Second != 0 {
				seconds++
			}
			h.Set("Retry-After", strconv.FormatInt(seconds, 10))
		} else if e.Status == 429 || e.Status == 503 {
			h.Set("Retry-After", "2")
		}
	}
	w.WriteHeader(e.Status)
	_, _ = w.Write(BodyWithRequestID(e, requestID))
}

// As 从任意 error 提取 Harbor Error；非 Harbor 错误返回内部错误。
func As(err error) *Error {
	if err == nil {
		return nil
	}
	var he *Error
	if errors.As(err, &he) {
		return he
	}
	// 超时类错误
	if errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "context deadline") {
		return New(ErrTimeout, err.Error())
	}
	return New(ErrInternal, err.Error())
}
