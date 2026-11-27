// SSE 事件级处理管道（同协议直通）。
//
// 设计原则：
//   - 同协议（chat/completions → chat/completions）优先直通，不重新序列化；
//   - 只在检测到需要标准化的字段（如 delta.reasoning）时改写该条 data 行；
//   - 按行拆帧且逐行写出，保持上游的增量时序，不做整段缓冲；
//   - 注释行、空行、无法解析的载荷原样透传，避免单帧异常中断整条流。
package compat

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"

	"harbor/internal/provider"
)

// wrapReasoningStream 返回一个逐行透传的 SSE 包装器：
// 仅把 data 行中的 delta.reasoning 转成标准 reasoning_content（并删除
// 冗余的 reasoning），同时保证思考分片的 delta 带 "content": null。
func wrapReasoningStream(src io.ReadCloser) io.ReadCloser {
	if src == nil {
		return nil
	}
	return &reasoningStream{src: src, br: bufio.NewReaderSize(src, 16<<10)}
}

type reasoningStream struct {
	src  io.ReadCloser
	br   *bufio.Reader
	out  bytes.Buffer
	err  error
	done bool
}

func (r *reasoningStream) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for r.out.Len() == 0 {
		if r.done {
			return 0, r.err
		}
		line, err, oversized := r.readLine()
		if !oversized && len(line) > 0 && err == nil {
			r.out.Write(frameChatLine(line))
		}
		if err != nil {
			r.done = true
			r.err = err
		}
		if r.out.Len() == 0 && r.done {
			return 0, r.err
		}
	}
	return r.out.Read(p)
}

// frameChatLine 对齐 agent2api 的 ReasoningCoalescer：每个完整行立即成帧。
// 空行跳过（由上一行补的 \n\n 已经承担事件边界），非 data 行原样成帧，
// data 行按需标准化 reasoning 后再补 \n\n。这样上游只发 "data: ...\n"
// 不发空行时，客户端仍能逐帧解析，不会攒到 EOF。
func frameChatLine(line []byte) []byte {
	trimmed := bytes.TrimRight(line, "\r\n")
	if len(bytes.TrimSpace(trimmed)) == 0 {
		return nil
	}
	if bytes.HasPrefix(trimmed, []byte("data:")) {
		trimmed = normalizeReasoningLine(trimmed)
	}
	out := make([]byte, 0, len(trimmed)+2)
	out = append(out, trimmed...)
	out = append(out, '\n', '\n')
	return out
}

// readLine 按行读取并限制解析缓冲；超长行直接原样写入 out，避免无限增长。
func (r *reasoningStream) readLine() (line []byte, err error, oversized bool) {
	for {
		frag, readErr := r.br.ReadSlice('\n')
		if len(frag) > 0 {
			if oversized || len(line)+len(frag) > maxBridgeSSEBytes {
				if !oversized {
					r.out.Write(line)
					line = nil
					oversized = true
				}
				r.out.Write(frag)
			} else {
				line = append(line, frag...)
			}
		}
		if readErr == bufio.ErrBufferFull {
			continue
		}
		return line, readErr, oversized
	}
}

func (r *reasoningStream) Close() error {
	if r.src == nil {
		return nil
	}
	return r.src.Close()
}

// StreamUsage 委托给内层 usage 旁路解析器（provider.streamUsageBody）。
func (r *reasoningStream) StreamUsage() (provider.Usage, bool) {
	if reader, ok := r.src.(provider.UsageReader); ok {
		return reader.StreamUsage()
	}
	return provider.Usage{}, false
}

// StreamModel 委托给内层解析器，保持审计中的 response_model 不变。
func (r *reasoningStream) StreamModel() string {
	if reader, ok := r.src.(interface{ StreamModel() string }); ok {
		return reader.StreamModel()
	}
	return ""
}

// TerminalFrames 返回某条入站协议的收尾帧特征（对齐 agent2api
// `api::pipeline::terminal`）。`stopEarly` 仅对 Chat 为 true：
// `data: [DONE]` 行本身就是完整事件，收到即可停止转发；
// Responses / Anthropic 的收尾是「event 行 + data 行」两行，提前停会截断
// data 行，所以只用于「客户端此刻断开算正常收尾」的判定。
func TerminalFrames(path string) (frames []string, stopEarly bool) {
	switch path {
	case "/v1/responses":
		return []string{"event: response.completed\n", "event: response.failed\n"}, false
	case "/v1/messages":
		return []string{"event: message_stop\n"}, false
	default:
		return []string{
			"data: [DONE]\n", "data:[DONE]\n",
			"data: [DONE]\r\n", "data:[DONE]\r\n",
		}, true
	}
}
func normalizeReasoningLine(line []byte) []byte {
	trimmed := bytes.TrimRight(line, "\r\n")
	suffix := line[len(trimmed):]
	if !bytes.HasPrefix(trimmed, []byte("data:")) || !bytes.Contains(trimmed, []byte(`"reasoning"`)) {
		return line
	}
	payload := bytes.TrimSpace(trimmed[len("data:"):])
	if len(payload) == 0 || payload[0] != '{' || bytes.Equal(payload, []byte("[DONE]")) {
		return line
	}
	doc, ok := decodePreservingNumbers(payload)
	if !ok {
		return line
	}
	changed := false
	if choices, ok := doc["choices"].([]any); ok {
		for _, item := range choices {
			choice, ok := item.(map[string]any)
			if !ok {
				continue
			}
			for _, field := range []string{"delta", "message"} {
				node, ok := choice[field].(map[string]any)
				if !ok {
					continue
				}
				reasoning, ok := node["reasoning"].(string)
				if !ok || reasoning == "" {
					continue
				}
				if existing, ok := node["reasoning_content"].(string); !ok || existing == "" {
					node["reasoning_content"] = reasoning
				}
				delete(node, "reasoning")
				changed = true
				// 严格客户端只在 delta 含 content 键时渲染思考分片。
				if field == "delta" {
					if _, exists := node["content"]; !exists {
						node["content"] = nil
					}
				}
			}
		}
	}
	if !changed {
		return line
	}
	encoded, err := json.Marshal(doc)
	if err != nil {
		return line
	}
	out := make([]byte, 0, len(encoded)+8)
	out = append(out, "data: "...)
	out = append(out, encoded...)
	out = append(out, suffix...)
	return out
}
