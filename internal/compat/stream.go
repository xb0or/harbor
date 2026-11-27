package compat

import (
	"bytes"
	"encoding/json"
	"io"
	"strconv"
	"strings"

	"harbor/internal/provider"
)

// Bound both an unterminated line and a multi-line SSE event.
const maxBridgeSSEBytes = 4 << 20

// Chat wire types track usage presence and upstream errors for both the
// synchronous Responses conversion and the incremental SSE bridge.
type streamChatDelta struct {
	Content          json.RawMessage       `json:"content"`
	ReasoningContent string                `json:"reasoning_content"`
	ToolCalls        []streamChatToolDelta `json:"tool_calls"`
}

type streamChatToolDelta struct {
	Index    int    `json:"index"`
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type streamChatChunk struct {
	ID      string          `json:"id"`
	Model   string          `json:"model"`
	Created int64           `json:"created"`
	Type    string          `json:"type"`
	Error   json.RawMessage `json:"error"`
	Choices []struct {
		Index        int             `json:"index"`
		FinishReason string          `json:"finish_reason"`
		Delta        streamChatDelta `json:"delta"`
		Message      streamChatDelta `json:"message"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens        int `json:"prompt_tokens"`
		CompletionTokens    int `json:"completion_tokens"`
		TotalTokens         int `json:"total_tokens"`
		PromptTokensDetails struct {
			CachedTokens int `json:"cached_tokens"`
		} `json:"prompt_tokens_details"`
		CompletionTokensDetails struct {
			ReasoningTokens int `json:"reasoning_tokens"`
		} `json:"completion_tokens_details"`
	} `json:"usage"`
}

type streamOutput struct {
	id        string
	typ       string
	index     int
	callID    string
	name      string
	text      strings.Builder
	announced bool
}

// Keep errors.Is useful to callers without exposing URLs, keys or prompts via Error().
type bridgeStreamError struct{ cause error }

func (e *bridgeStreamError) Error() string { return "Upstream stream failed" }
func (e *bridgeStreamError) Unwrap() error { return e.cause }

type streamBridge struct {
	src             io.ReadCloser
	kind            Kind
	buf             bytes.Buffer
	pending         []byte
	eventData       []byte
	eventName       string
	started         bool
	done            bool
	sawDone         bool
	terminalErr     error
	readErr         error
	idleReads       int
	finishReason    string
	id              string
	model           string
	created         int64
	message         *streamOutput
	reasoning       *streamOutput
	tools           map[int]*streamOutput
	outputs         []*streamOutput
	sequence        int
	usage           provider.Usage
	known           bool
	cachedTokens    int
	reasoningTokens int
}

func wrapChatStream(kind Kind, src io.ReadCloser) io.ReadCloser {
	if src == nil || kind == KindNone {
		return src
	}
	return &streamBridge{src: src, kind: kind}
}

func (b *streamBridge) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for b.buf.Len() == 0 {
		if b.done {
			if b.terminalErr != nil {
				return 0, b.terminalErr
			}
			return 0, io.EOF
		}
		b.pull()
	}
	return b.buf.Read(p)
}

func (b *streamBridge) Close() error {
	if b.src == nil {
		return nil
	}
	return b.src.Close()
}

func (b *streamBridge) StreamUsage() (provider.Usage, bool) {
	if r, ok := b.src.(provider.UsageReader); ok {
		if u, known := r.StreamUsage(); known {
			return u, true
		}
	}
	return b.usage, b.known
}

func (b *streamBridge) pull() {
	var tmp [4096]byte
	n, err := b.src.Read(tmp[:])
	if err != nil && err != io.EOF {
		b.readErr = err
	}
	if n > 0 {
		b.idleReads = 0
		b.consume(tmp[:n])
	} else if err == nil {
		b.idleReads++
		if b.idleReads >= 100 {
			b.fail(io.ErrNoProgress)
		}
	}
	if b.done {
		return
	}
	// A Reader may return data AND an error. Do not emit success while
	// parsing those bytes, even if they contain finish_reason or [DONE].
	if b.readErr != nil {
		b.fail(b.readErr)
		return
	}
	if err == io.EOF {
		if len(b.pending) > 0 && !b.sawDone {
			b.handleLine(b.pending)
			b.pending = nil
		}
		if !b.done && !b.sawDone {
			b.dispatchEvent()
		}
		if b.done {
			return
		}
		if !b.sawDone && b.finishReason == "" {
			b.fail(io.ErrUnexpectedEOF)
			return
		}
	}
	if b.sawDone || err == io.EOF {
		b.finish()
	}
}

func (b *streamBridge) consume(data []byte) {
	for len(data) > 0 && !b.done && !b.sawDone {
		i := bytes.IndexByte(data, '\n')
		end := i
		if end < 0 {
			end = len(data)
		}
		if len(b.pending)+end > maxBridgeSSEBytes {
			b.fail(nil)
			return
		}
		b.pending = append(b.pending, data[:end]...)
		if i < 0 {
			return
		}
		b.handleLine(b.pending)
		b.pending = b.pending[:0]
		data = data[i+1:]
	}
}

func (b *streamBridge) handleLine(line []byte) {
	line = bytes.TrimSuffix(line, []byte("\r"))
	if len(line) == 0 {
		b.dispatchEvent()
		return
	}
	if line[0] == ':' {
		return
	}
	field, value, _ := bytes.Cut(line, []byte(":"))
	value = bytes.TrimPrefix(value, []byte(" "))
	switch string(field) {
	case "event":
		b.eventName = string(value)
	case "data":
		if len(b.eventData)+len(value)+1 > maxBridgeSSEBytes {
			b.fail(nil)
			return
		}
		b.eventData = append(b.eventData, value...)
		b.eventData = append(b.eventData, '\n')
	}
}

func (b *streamBridge) dispatchEvent() {
	data := bytes.TrimSpace(b.eventData)
	eventName := b.eventName
	b.eventData = b.eventData[:0]
	b.eventName = ""
	if eventName == "error" {
		b.fail(nil)
		return
	}
	if len(data) == 0 {
		return
	}
	if bytes.Equal(data, []byte("[DONE]")) {
		b.sawDone = true
		return
	}
	var chunk streamChatChunk
	if data[0] != '{' || json.Unmarshal(data, &chunk) != nil {
		b.fail(nil)
		return
	}
	if chunk.Type == "error" || (len(chunk.Error) > 0 && !bytes.Equal(bytes.TrimSpace(chunk.Error), []byte("null"))) {
		b.fail(nil)
		return
	}
	if !b.started {
		if chunk.ID != "" {
			b.id = chunk.ID
		}
		if chunk.Model != "" {
			b.model = chunk.Model
		}
		if chunk.Created != 0 {
			b.created = chunk.Created
		}
	}
	if u := chunk.Usage; u != nil {
		b.usage = provider.Usage{PromptTokens: u.PromptTokens, CompletionTokens: u.CompletionTokens, TotalTokens: u.TotalTokens}
		if b.usage.TotalTokens == 0 {
			b.usage.TotalTokens = b.usage.PromptTokens + b.usage.CompletionTokens
		}
		b.cachedTokens = u.PromptTokensDetails.CachedTokens
		b.reasoningTokens = u.CompletionTokensDetails.ReasoningTokens
		b.known = true
	}
	for _, choice := range chunk.Choices {
		if choice.Index != 0 {
			continue
		}
		b.emitStart()
		delta := choice.Delta
		text := rawText(delta.Content)
		if text == "" {
			text = rawText(choice.Message.Content)
		}
		if delta.ReasoningContent == "" {
			delta.ReasoningContent = choice.Message.ReasoningContent
		}
		if len(delta.ToolCalls) == 0 {
			delta.ToolCalls = choice.Message.ToolCalls
		}
		if b.kind == KindResponses {
			if delta.ReasoningContent != "" {
				b.emitText("reasoning", delta.ReasoningContent)
			}
			if text != "" {
				b.emitText("message", text)
			}
			for _, tool := range delta.ToolCalls {
				if tool.Index < 0 || (tool.Type != "" && tool.Type != "function") {
					b.fail(nil)
					return
				}
				b.emitTool(tool)
			}
		} else if text != "" {
			b.writeEvent("content_block_delta", map[string]any{
				"index": 0, "delta": map[string]any{"type": "text_delta", "text": stripDSMLLeaks(text)},
			})
		}
		// Usage-only chunks often arrive in later reads, after this marker.
		if choice.FinishReason != "" {
			b.finishReason = choice.FinishReason
		}
		break
	}
}

func (b *streamBridge) emitStart() {
	if b.started {
		return
	}
	b.started = true
	b.id = responseID(b.id)
	switch b.kind {
	case KindResponses:
		b.writeEvent("response.created", map[string]any{"response": b.responseSnapshot("in_progress", false)})
		b.writeEvent("response.in_progress", map[string]any{"response": b.responseSnapshot("in_progress", false)})
	case KindAnthropic:
		b.writeEvent("message_start", map[string]any{
			"message": map[string]any{
				"id": b.id, "type": "message", "role": "assistant", "model": b.model, "content": []any{},
			},
		})
		b.writeEvent("content_block_start", map[string]any{
			"index": 0, "content_block": map[string]any{"type": "text", "text": ""},
		})
	}
}

func (b *streamBridge) newOutput(typ string) *streamOutput {
	prefix := map[string]string{"message": "msg_", "reasoning": "rs_", "function_call": "fc_"}[typ]
	item := &streamOutput{
		id:  prefix + strings.TrimPrefix(b.id, "resp_") + "_" + strconv.Itoa(len(b.outputs)),
		typ: typ, index: len(b.outputs),
	}
	b.outputs = append(b.outputs, item)
	return item
}

func (b *streamBridge) announce(item *streamOutput) {
	item.announced = true
	b.writeEvent("response.output_item.added", map[string]any{
		"output_index": item.index, "item": item.snapshot("in_progress", false),
	})
	if item.typ == "message" {
		b.writeItemEvent("response.content_part.added", item, map[string]any{"part": streamPart(item.typ, "")})
	} else if item.typ == "reasoning" {
		b.writeItemEvent("response.reasoning_summary_part.added", item, map[string]any{"part": streamPart(item.typ, "")})
	}
}

func (b *streamBridge) emitText(typ, delta string) {
	// 上游在超长 agent 场景可能把 DeepSeek 私有 D$ML DSL 标签泄进 content 或
	// reasoning；统一剥离，避免思考块与正文残留 </ | D$ML | …> 尾巴。
	delta = stripDSMLLeaks(delta)
	item := b.message
	event := "response.output_text.delta"
	if typ == "reasoning" {
		item = b.reasoning
		event = "response.reasoning_summary_text.delta"
	}
	if item == nil {
		item = b.newOutput(typ)
		if typ == "reasoning" {
			b.reasoning = item
		} else {
			b.message = item
		}
		b.announce(item)
	}
	item.text.WriteString(delta)
	b.writeItemEvent(event, item, map[string]any{"delta": delta})
}

func (b *streamBridge) emitTool(delta streamChatToolDelta) {
	if b.tools == nil {
		b.tools = make(map[int]*streamOutput)
	}
	item := b.tools[delta.Index]
	if item == nil {
		item = b.newOutput("function_call")
		b.tools[delta.Index] = item
	}
	if delta.ID != item.callID {
		item.callID += delta.ID
	}
	item.name += delta.Function.Name
	item.text.WriteString(delta.Function.Arguments)
	args := delta.Function.Arguments
	if !item.announced {
		// Some providers send arguments before the call's identity. Buffer
		// them until an addressable function_call item can be announced.
		if item.callID == "" || item.name == "" {
			return
		}
		b.announce(item)
		args = item.text.String()
	}
	if args != "" {
		b.writeItemEvent("response.function_call_arguments.delta", item, map[string]any{"delta": args})
	}
}

func streamPart(typ, text string) map[string]any {
	if typ == "reasoning" {
		return map[string]any{"type": "summary_text", "text": text}
	}
	return map[string]any{"type": "output_text", "text": text, "annotations": []any{}}
}

func (item *streamOutput) snapshot(status string, final bool) map[string]any {
	out := map[string]any{"id": item.id, "type": item.typ, "status": status}
	switch item.typ {
	case "message", "reasoning":
		parts := []any{}
		if final {
			parts = append(parts, streamPart(item.typ, item.text.String()))
		}
		if item.typ == "message" {
			out["role"], out["content"] = "assistant", parts
		} else {
			out["summary"] = parts
		}
	case "function_call":
		out["call_id"], out["name"], out["arguments"] = item.callID, item.name, ""
		if final {
			out["arguments"] = item.text.String()
		}
	}
	return out
}

func (b *streamBridge) responseSnapshot(status string, final bool) map[string]any {
	output := make([]any, 0, len(b.outputs))
	if final {
		itemStatus := status
		if itemStatus == "failed" {
			itemStatus = "incomplete"
		}
		for _, item := range b.outputs {
			output = append(output, item.snapshot(itemStatus, true))
		}
	}
	response := map[string]any{
		"id": b.id, "object": "response", "created_at": b.created, "status": status,
		"model": b.model, "output": output, "error": nil, "incomplete_details": nil, "usage": nil,
	}
	if final {
		usage, _ := b.StreamUsage()
		response["usage"] = map[string]any{
			"input_tokens": usage.PromptTokens, "output_tokens": usage.CompletionTokens, "total_tokens": usage.TotalTokens,
			"input_tokens_details":  map[string]any{"cached_tokens": b.cachedTokens},
			"output_tokens_details": map[string]any{"reasoning_tokens": b.reasoningTokens},
		}
	}
	return response
}

func (b *streamBridge) finish() {
	if b.done {
		return
	}
	b.emitStart()
	status, reason := "completed", ""
	switch b.finishReason {
	case "length":
		status, reason = "incomplete", "max_output_tokens"
	case "content_filter":
		status, reason = "incomplete", "content_filter"
	}
	switch b.kind {
	case KindResponses:
		for _, item := range b.outputs {
			if !item.announced {
				b.fail(nil)
				return
			}
		}
		for _, item := range b.outputs {
			switch item.typ {
			case "message":
				b.writeItemEvent("response.output_text.done", item, map[string]any{"text": item.text.String()})
				b.writeItemEvent("response.content_part.done", item, map[string]any{"part": streamPart(item.typ, item.text.String())})
			case "reasoning":
				b.writeItemEvent("response.reasoning_summary_text.done", item, map[string]any{"text": item.text.String()})
				b.writeItemEvent("response.reasoning_summary_part.done", item, map[string]any{"part": streamPart(item.typ, item.text.String())})
			case "function_call":
				b.writeItemEvent("response.function_call_arguments.done", item, map[string]any{"arguments": item.text.String(), "name": item.name})
			}
			b.writeEvent("response.output_item.done", map[string]any{"output_index": item.index, "item": item.snapshot(status, true)})
		}
		response := b.responseSnapshot(status, true)
		if reason != "" {
			response["incomplete_details"] = map[string]any{"reason": reason}
		}
		b.writeEvent("response."+status, map[string]any{"response": response})
		b.buf.WriteString("data: [DONE]\n\n")
	case KindAnthropic:
		stop := "end_turn"
		if b.finishReason == "length" {
			stop = "max_tokens"
		}
		b.writeEvent("content_block_stop", map[string]any{"index": 0})
		usage, _ := b.StreamUsage()
		b.writeEvent("message_delta", map[string]any{
			"delta": map[string]any{"stop_reason": stop}, "usage": map[string]any{"output_tokens": usage.CompletionTokens},
		})
		b.writeEvent("message_stop", map[string]any{})
	}
	b.done = true
	b.pending, b.eventData = nil, nil
}

func (b *streamBridge) fail(cause error) {
	if b.done {
		return
	}
	if b.readErr != nil {
		cause = b.readErr
	}
	b.terminalErr = &bridgeStreamError{cause: cause}
	b.emitStart()
	if b.kind == KindResponses {
		response := b.responseSnapshot("failed", true)
		response["error"] = map[string]any{"code": "upstream_error", "message": b.terminalErr.Error()}
		b.writeEvent("response.failed", map[string]any{"response": response})
	} else {
		b.writeEvent("error", map[string]any{"error": map[string]any{"type": "api_error", "message": b.terminalErr.Error()}})
	}
	b.done = true
	b.pending, b.eventData = nil, nil
}

func (b *streamBridge) writeItemEvent(event string, item *streamOutput, payload map[string]any) {
	payload["item_id"], payload["output_index"] = item.id, item.index
	if item.typ == "message" {
		payload["content_index"] = 0
	}
	if item.typ == "reasoning" {
		payload["summary_index"] = 0
	}
	b.writeEvent(event, payload)
}

func (b *streamBridge) writeEvent(event string, payload map[string]any) {
	payload["type"] = event
	if b.kind == KindResponses {
		payload["sequence_number"] = b.sequence
		b.sequence++
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	b.buf.WriteString("event: ")
	b.buf.WriteString(event)
	b.buf.WriteString("\ndata: ")
	b.buf.Write(raw)
	b.buf.WriteString("\n\n")
}
