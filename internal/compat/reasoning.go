// 思考字段兼容层。
//
// 部分 OpenAI 兼容上游把思考文本放在 delta.reasoning / message.reasoning，
// 而 OpenAI 标准与多数客户端只认 reasoning_content。字段缺失时客户端会
// 认为"没有思考内容"，表现为思考阶段界面空白、正文一次性出现。
// 这里在不改变其它字段的前提下补一份 reasoning_content。
package compat

import (
	"bytes"
	"encoding/json"
	"io"

	"harbor/internal/provider"
)

// maxReasoningLine 限制单条待处理 SSE 行的长度，超过则原样透传。
const maxReasoningLine = 4 << 20

// reasoningNeedle 用于快速跳过不含思考字段的行。
var reasoningNeedle = []byte(`"reasoning"`)

// AliasReasoningBody 为普通（非流式）响应补 reasoning_content。
// 解析失败或无需修改时原样返回，绝不破坏上游内容。
func AliasReasoningBody(body []byte) []byte {
	if len(body) == 0 || !bytes.Contains(body, reasoningNeedle) {
		return body
	}
	doc, ok := decodePreservingNumbers(body)
	if !ok {
		return body
	}
	changed := false
	if choices, ok := doc["choices"].([]any); ok {
		for _, item := range choices {
			choice, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if message, ok := choice["message"].(map[string]any); ok && aliasDelta(message) {
				changed = true
			}
			if delta, ok := choice["delta"].(map[string]any); ok && aliasDelta(delta) {
				changed = true
			}
		}
	}
	if !changed {
		return body
	}
	encoded, err := json.Marshal(doc)
	if err != nil {
		return body
	}
	return encoded
}

// aliasDelta 在某条消息对象上补齐 reasoning_content，返回是否修改。
func aliasDelta(node map[string]any) bool {
	if _, exists := node["reasoning_content"]; exists {
		return false
	}
	reasoning, ok := node["reasoning"].(string)
	if !ok || reasoning == "" {
		return false
	}
	node["reasoning_content"] = reasoning
	return true
}

// decodePreservingNumbers 解析 JSON 且保留数字字面量，避免 created 等字段被改写成科学计数法。
func decodePreservingNumbers(body []byte) (map[string]any, bool) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var doc map[string]any
	if err := decoder.Decode(&doc); err != nil {
		return nil, false
	}
	return doc, true
}

// aliasStream 逐行改写 SSE，完整事件立即输出，不缓存跨事件数据。
type aliasStream struct {
	src     io.ReadCloser
	pending []byte
	out     []byte
	done    bool
	err     error
}

// AliasReasoningStream 包装流式响应体，把每行的 reasoning 同步成 reasoning_content。
func AliasReasoningStream(src io.ReadCloser) io.ReadCloser {
	if src == nil {
		return src
	}
	return &aliasStream{src: src}
}

// StreamUsage 透传上游已解析的 token 用量，避免审计丢数据。
func (s *aliasStream) StreamUsage() (provider.Usage, bool) {
	if reader, ok := s.src.(provider.UsageReader); ok {
		return reader.StreamUsage()
	}
	return provider.Usage{}, false
}

// StreamModel 透传上游回显的模型名。
func (s *aliasStream) StreamModel() string {
	if reader, ok := s.src.(interface{ StreamModel() string }); ok {
		return reader.StreamModel()
	}
	return ""
}

// Close 关闭底层响应体。
func (s *aliasStream) Close() error { return s.src.Close() }

// Read 按行处理并立即返回已改写的数据，不等待 EOF。
func (s *aliasStream) Read(p []byte) (int, error) {
	for len(s.out) == 0 {
		if s.done {
			return 0, s.err
		}
		buf := make([]byte, 32*1024)
		n, err := s.src.Read(buf)
		if n > 0 {
			s.pending = append(s.pending, buf[:n]...)
			s.drain()
		}
		if err != nil {
			if len(s.pending) > 0 {
				s.out = append(s.out, rewriteReasoningLine(s.pending)...)
				s.pending = nil
			}
			s.done = true
			s.err = err
			if len(s.out) == 0 {
				return 0, err
			}
			break
		}
	}
	n := copy(p, s.out)
	s.out = s.out[n:]
	return n, nil
}

// drain 把 pending 中已完整的行改写后移入输出缓冲。
func (s *aliasStream) drain() {
	for {
		index := bytes.IndexByte(s.pending, '\n')
		if index < 0 {
			// 超长行不无限累积：原样吐出，交给下游处理。
			if len(s.pending) > maxReasoningLine {
				s.out = append(s.out, s.pending...)
				s.pending = nil
			}
			return
		}
		s.out = append(s.out, rewriteReasoningLine(s.pending[:index+1])...)
		s.pending = s.pending[index+1:]
	}
}

// rewriteReasoningLine 只改写以 data: 开头的 JSON 行，其它字节保持不变。
func rewriteReasoningLine(line []byte) []byte {
	if !bytes.Contains(line, reasoningNeedle) {
		return line
	}
	trimmed := bytes.TrimSuffix(line, []byte("\n"))
	trimmed = bytes.TrimSuffix(trimmed, []byte("\r"))
	if !bytes.HasPrefix(trimmed, []byte("data:")) {
		return line
	}
	payload := bytes.TrimPrefix(trimmed, []byte("data:"))
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 || payload[0] != '{' {
		return line
	}
	rewritten := AliasReasoningBody(payload)
	if bytes.Equal(rewritten, payload) {
		return line
	}
	suffix := line[len(trimmed):]
	out := make([]byte, 0, len(rewritten)+8)
	out = append(out, []byte("data: ")...)
	out = append(out, rewritten...)
	out = append(out, suffix...)
	return out
}
