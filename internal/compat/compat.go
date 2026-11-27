// Package compat 把客户端新协议桥到 openai-compatible 上游真正实现的路径。
//
// Harbor 对外接受 /v1/responses（OpenAI Responses）和 /v1/messages（Anthropic），
// 但绝大多数兼容网关（airouter / LiteLLM / OneAPI）只实现 /v1/chat/completions。
// 原样转发会得到 HTML 404，再被收敛成 502 upstream_error。
package compat

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"harbor/internal/provider"
)

// Kind 是入站协议到 chat/completions 的桥类型。
type Kind int

const (
	KindNone Kind = iota
	KindResponses
	KindAnthropic
)

const chatCompletionsPath = "/v1/chat/completions"

var responsesDrop = []string{
	"background",
	"conversation",
	"context_management",
	"include",
	"max_tool_calls",
	"previous_response_id",
	"prompt",
	"prompt_cache_key",
	"reasoning",
	"safety_identifier",
	"service_tier",
	"store",
	"text",
	"truncation",
}

var anthropicDrop = []string{
	"cache_control",
	"container",
	"context_management",
	"mcp_servers",
	"metadata",
	"thinking",
}

// RewriteRequest 把入站 path/body 改成 openai-compatible 上游能吃的形态。
// chat/completions、embeddings 原样返回。
func RewriteRequest(inboundPath string, body []byte) (string, []byte, Kind, error) {
	switch inboundPath {
	case "/v1/responses":
		out, err := responsesToChat(body)
		if err != nil {
			return inboundPath, body, KindNone, err
		}
		return chatCompletionsPath, out, KindResponses, nil
	case "/v1/messages":
		out, err := anthropicToChat(body)
		if err != nil {
			return inboundPath, body, KindNone, err
		}
		return chatCompletionsPath, out, KindAnthropic, nil
	default:
		return inboundPath, body, KindNone, nil
	}
}

// ApplyResponse 把上游 chat/completions 成功响应翻回客户端入站协议。
// 转换失败必须报错，不能把错误对象或 HTML 当作 200 成功透传。
func ApplyResponse(kind Kind, resp *provider.Response) error {
	if resp == nil {
		return nil
	}
	if kind == KindNone {
		// 原生 chat/completions：同协议直通优先。
		// 流式只做「单次按行拆帧」的思考字段标准化，其余事件字节原样透传；
		// 非流式才整体补齐思考字段（重新序列化会改变 delta 字段集合）。
		if resp.Stream {
			if resp.StreamBody != nil {
				resp.StreamBody = wrapReasoningStream(resp.StreamBody)
			}
			return nil
		}
		resp.Body = AliasReasoningBody(resp.Body)
		return nil
	}
	if resp.Stream {
		if resp.StreamBody == nil {
			return fmt.Errorf("upstream stream body is missing")
		}
		resp.StreamBody = wrapChatStream(kind, resp.StreamBody)
		return nil
	}
	var out []byte
	var err error
	switch kind {
	case KindResponses:
		out, err = chatToResponses(resp.Body)
	case KindAnthropic:
		out, err = chatToAnthropic(resp.Body)
	default:
		return nil
	}
	if err != nil {
		return fmt.Errorf("invalid upstream response for protocol bridge")
	}
	resp.Body = out
	return nil
}

func responsesToChat(body []byte) ([]byte, error) {
	raw, err := decodeObject(body)
	if err != nil {
		return nil, err
	}

	var messages []any
	if inst, ok := raw["instructions"]; ok {
		if s := decodeString(inst); s != "" {
			messages = append(messages, map[string]any{"role": "system", "content": s})
		}
	}

	if input, ok := raw["input"]; ok {
		converted, err := inputToMessages(input)
		if err != nil {
			return nil, err
		}
		messages = append(messages, converted...)
	}

	if existing, ok := raw["messages"]; ok && len(messages) == 0 {
		if err := json.Unmarshal(existing, &messages); err != nil {
			return nil, fmt.Errorf("responses messages: %w", err)
		}
	}

	encoded, err := json.Marshal(messages)
	if err != nil {
		return nil, err
	}
	raw["messages"] = encoded
	delete(raw, "input")
	delete(raw, "instructions")

	if mot, ok := raw["max_output_tokens"]; ok {
		if _, exists := raw["max_tokens"]; !exists {
			raw["max_tokens"] = mot
		}
		delete(raw, "max_output_tokens")
	}
	if err := responsesOptionsToChat(raw, decodeString(raw["model"])); err != nil {
		return nil, err
	}
	for _, key := range responsesDrop {
		delete(raw, key)
	}
	return encodeObject(raw)
}

// Responses uses flat function definitions and different structured-output/stream
// options. Passing these through unchanged causes upstream 400s.
func responsesOptionsToChat(raw map[string]json.RawMessage, model string) error {
	for _, key := range []string{"previous_response_id", "conversation", "prompt"} {
		value := bytes.TrimSpace(raw[key])
		if len(value) > 0 && string(value) != "null" && string(value) != `""` {
			return fmt.Errorf("responses %s requires server-side state; send full input instead", key)
		}
	}
	if tools, ok := raw["tools"]; ok {
		var items []map[string]json.RawMessage
		if err := json.Unmarshal(tools, &items); err != nil {
			return fmt.Errorf("responses tools must be an array")
		}
		for i, tool := range items {
			if decodeString(tool["type"]) != "function" {
				return fmt.Errorf("responses tool %d is not a supported function", i)
			}
			fn := tool["function"]
			if len(fn) == 0 {
				fields := make(map[string]json.RawMessage)
				for _, key := range []string{"name", "description", "parameters", "strict"} {
					if value, ok := tool[key]; ok {
						fields[key] = value
					}
				}
				fn, _ = json.Marshal(fields)
			}
			obj, err := decodeObject(fn)
			if err != nil || decodeString(obj["name"]) == "" {
				return fmt.Errorf("responses tool %d requires a function name", i)
			}
			items[i] = map[string]json.RawMessage{"type": json.RawMessage(`"function"`), "function": fn}
		}
		raw["tools"], _ = json.Marshal(items)
	}
	if choice := bytes.TrimSpace(raw["tool_choice"]); len(choice) > 0 && choice[0] == '{' {
		obj, err := decodeObject(choice)
		if err != nil || decodeString(obj["type"]) != "function" {
			return fmt.Errorf("unsupported responses tool_choice")
		}
		fn := obj["function"]
		if len(fn) == 0 {
			fn, _ = json.Marshal(map[string]json.RawMessage{"name": obj["name"]})
		}
		fields, err := decodeObject(fn)
		if err != nil || decodeString(fields["name"]) == "" {
			return fmt.Errorf("responses tool_choice requires a function name")
		}
		raw["tool_choice"], _ = json.Marshal(map[string]json.RawMessage{"type": json.RawMessage(`"function"`), "function": fn})
	}
	if reasoning, ok := raw["reasoning"]; ok {
		obj, err := decodeObject(reasoning)
		if err != nil {
			return fmt.Errorf("invalid responses reasoning")
		}
		effort := decodeString(obj["effort"])
		if effort == "" || effort == "none" {
			// 无思考要求：不向上游声明任何 thinking/reasoning 字段。
			if isDeepSeekModel(model) {
				delete(raw, "thinking")
			}
			return nil
		}
		if isDeepSeekModel(model) {
			// DeepSeek 系上游认 thinking 参数（OpenAI 风格 reasoning_effort 会被 400）。
			raw["thinking"] = deepSeekThinking(effort)
		} else {
			raw["reasoning_effort"] = obj["effort"]
		}
	}
	if text, ok := raw["text"]; ok {
		obj, err := decodeObject(text)
		if err != nil {
			return fmt.Errorf("invalid responses text options")
		}
		if format, ok := obj["format"]; ok {
			fields, err := decodeObject(format)
			if err != nil {
				return fmt.Errorf("invalid responses text format")
			}
			if decodeString(fields["type"]) == "json_schema" {
				delete(fields, "type")
				schema, _ := json.Marshal(fields)
				format, _ = json.Marshal(map[string]json.RawMessage{"type": json.RawMessage(`"json_schema"`), "json_schema": schema})
			}
			raw["response_format"] = format
		}
	}
	// Responses always includes usage; request Chat's trailing usage chunk.
	// include_obfuscation is a Responses-only option and must not leak upstream.
	if bytes.Equal(bytes.TrimSpace(raw["stream"]), []byte("true")) {
		raw["stream_options"] = json.RawMessage(`{"include_usage":true}`)
	} else {
		delete(raw, "stream_options")
	}
	return nil
}

// isDeepSeekModel 判断上游模型是否为 DeepSeek 系。
// DeepSeek 系 chat API 认 thinking 参数；OpenAI 风格 reasoning_effort 会被 400。
func isDeepSeekModel(model string) bool {
	return strings.Contains(strings.ToLower(model), "deepseek")
}

// deepSeekThinking 把 responses reasoning.effort 映射为 DeepSeek thinking 参数。
// budget_tokens 取值参考 DeepSeek 官方思考模式区间 [1, 8192]（部分新模型支持更大值）。
func deepSeekThinking(effort string) json.RawMessage {
	budget := map[string]int{
		"low":    1024,
		"medium": 2048,
		"high":   4096,
		"max":    8192,
	}[strings.ToLower(effort)]
	if budget == 0 {
		budget = 1024
	}
	// 手动构造，避免引入额外转义层。
	return json.RawMessage(fmt.Sprintf(`{"type":"enabled","budget_tokens":%d}`, budget))
}

func anthropicToChat(body []byte) ([]byte, error) {
	raw, err := decodeObject(body)
	if err != nil {
		return nil, err
	}

	var messages []any
	if sys, ok := raw["system"]; ok {
		if s := anthropicSystemText(sys); s != "" {
			messages = append(messages, map[string]any{"role": "system", "content": s})
		}
	}
	if existing, ok := raw["messages"]; ok {
		var rest []any
		if err := json.Unmarshal(existing, &rest); err != nil {
			return nil, fmt.Errorf("anthropic messages: %w", err)
		}
		for i, msg := range rest {
			rest[i] = normalizeAnthropicMessage(msg)
		}
		messages = append(messages, rest...)
	}
	encoded, err := json.Marshal(messages)
	if err != nil {
		return nil, err
	}
	raw["messages"] = encoded
	delete(raw, "system")

	if seq, ok := raw["stop_sequences"]; ok {
		if _, exists := raw["stop"]; !exists {
			raw["stop"] = seq
		}
		delete(raw, "stop_sequences")
	}
	for _, key := range anthropicDrop {
		delete(raw, key)
	}
	return encodeObject(raw)
}

func inputToMessages(raw json.RawMessage) ([]any, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, nil
	}
	switch raw[0] {
	case '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, err
		}
		if s == "" {
			return nil, nil
		}
		return []any{map[string]any{"role": "user", "content": s}}, nil
	case '{':
		msg, err := itemToMessage(raw)
		if err != nil || msg == nil {
			return nil, err
		}
		return []any{msg}, nil
	case '[':
		var items []json.RawMessage
		if err := json.Unmarshal(raw, &items); err != nil {
			return nil, err
		}
		out := make([]any, 0, len(items))
		for _, item := range items {
			item = bytes.TrimSpace(item)
			if len(item) == 0 {
				continue
			}
			if item[0] == '"' {
				var s string
				if err := json.Unmarshal(item, &s); err != nil {
					return nil, err
				}
				out = append(out, map[string]any{"role": "user", "content": s})
				continue
			}
			msg, err := itemToMessage(item)
			if err != nil {
				return nil, err
			}
			if msg != nil {
				out = appendResponseMessage(out, msg)
			}
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported responses input")
	}
}

// A Responses assistant turn is split into reasoning/message/function_call
// items. Chat requires parallel calls in ONE assistant message before results.
func appendResponseMessage(out []any, msg any) []any {
	current := msg.(map[string]any)
	if len(out) > 0 && current["role"] == "assistant" {
		last, ok := out[len(out)-1].(map[string]any)
		calls, _ := current["tool_calls"].([]any)
		reasoning, _ := current["reasoning_content"].(string)
		if ok && last["role"] == "assistant" && (len(calls) > 0 || reasoning != "" || last["content"] == nil) {
			if len(calls) > 0 {
				existing, _ := last["tool_calls"].([]any)
				last["tool_calls"] = append(existing, calls...)
			}
			if reasoning != "" {
				previous, _ := last["reasoning_content"].(string)
				last["reasoning_content"] = previous + reasoning
			}
			if current["content"] != nil {
				last["content"] = current["content"]
			}
			return out
		}
	}
	return append(out, msg)
}

func itemToMessage(raw json.RawMessage) (any, error) {
	obj, err := decodeObject(raw)
	if err != nil {
		return nil, err
	}
	typ := decodeString(obj["type"])
	role := decodeString(obj["role"])
	if role == "" {
		role = "user"
	}
	if role == "developer" {
		role = "system"
	}
	switch typ {
	case "", "message":
		content, err := normalizeContent(obj["content"])
		if err != nil {
			return nil, err
		}
		return map[string]any{"role": role, "content": content}, nil
	case "input_text":
		return map[string]any{"role": "user", "content": decodeString(obj["text"])}, nil
	case "reasoning":
		var parts []struct {
			Text string `json:"text"`
		}
		if summary, ok := obj["summary"]; ok {
			if err := json.Unmarshal(summary, &parts); err != nil {
				return nil, fmt.Errorf("invalid reasoning summary")
			}
		}
		var text strings.Builder
		for _, part := range parts {
			text.WriteString(part.Text)
		}
		if text.Len() == 0 {
			return nil, nil
		}
		return map[string]any{"role": "assistant", "content": nil, "reasoning_content": text.String()}, nil
	case "function_call":
		call := map[string]any{
			"id":   decodeString(obj["call_id"]),
			"type": "function",
			"function": map[string]any{
				"name":      decodeString(obj["name"]),
				"arguments": decodeString(obj["arguments"]),
			},
		}
		if call["id"] == "" {
			call["id"] = decodeString(obj["id"])
		}
		if call["id"] == "" || decodeString(obj["name"]) == "" {
			return nil, fmt.Errorf("function_call requires call_id and name")
		}
		return map[string]any{"role": "assistant", "content": nil, "tool_calls": []any{call}}, nil
	case "function_call_output":
		content, err := normalizeContent(obj["output"])
		if err != nil {
			return nil, fmt.Errorf("invalid function_call_output content")
		}
		msg := map[string]any{
			"role":         "tool",
			"content":      content,
			"tool_call_id": decodeString(obj["call_id"]),
		}
		if msg["tool_call_id"] == "" {
			msg["tool_call_id"] = decodeString(obj["id"])
		}
		if msg["tool_call_id"] == "" {
			return nil, fmt.Errorf("function_call_output requires call_id")
		}
		return msg, nil
	default:
		return nil, fmt.Errorf("unsupported responses input item type")
	}
}

func normalizeContent(raw json.RawMessage) (any, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return "", nil
	}
	if raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, err
		}
		return s, nil
	}
	if raw[0] != '[' {
		return json.RawMessage(append([]byte(nil), raw...)), nil
	}
	var parts []map[string]any
	if err := json.Unmarshal(raw, &parts); err != nil {
		return nil, err
	}
	texts := make([]string, 0, len(parts))
	converted := make([]any, 0, len(parts))
	onlyText := true
	for _, part := range parts {
		switch fmt.Sprint(part["type"]) {
		case "input_text", "output_text", "text":
			text := fmt.Sprint(part["text"])
			texts = append(texts, text)
			converted = append(converted, map[string]any{"type": "text", "text": text})
		case "input_image", "image_url":
			onlyText = false
			converted = append(converted, chatImagePart(part))
		default:
			onlyText = false
			converted = append(converted, part)
		}
	}
	if onlyText {
		return strings.Join(texts, ""), nil
	}
	return converted, nil
}

func chatImagePart(part map[string]any) map[string]any {
	switch url := part["image_url"].(type) {
	case string:
		return map[string]any{"type": "image_url", "image_url": map[string]any{"url": url}}
	case map[string]any:
		return map[string]any{"type": "image_url", "image_url": url}
	}
	if url, ok := part["url"].(string); ok && url != "" {
		return map[string]any{"type": "image_url", "image_url": map[string]any{"url": url}}
	}
	return map[string]any{"type": "image_url", "image_url": map[string]any{"url": ""}}
}

func chatToResponses(body []byte) ([]byte, error) {
	var src streamChatChunk
	if err := json.Unmarshal(body, &src); err != nil {
		return nil, err
	}
	if len(src.Choices) == 0 || (len(src.Error) > 0 && !bytes.Equal(bytes.TrimSpace(src.Error), []byte("null"))) {
		return nil, fmt.Errorf("invalid upstream chat completion")
	}
	choice := src.Choices[0]
	status, reason := "completed", ""
	switch choice.FinishReason {
	case "length":
		status, reason = "incomplete", "max_output_tokens"
	case "content_filter":
		status, reason = "incomplete", "content_filter"
	}
	id := responseID(src.ID)
	output := make([]any, 0)
	appendText := func(typ, prefix, text string) {
		item := &streamOutput{id: prefix + strings.TrimPrefix(id, "resp_"), typ: typ}
		item.text.WriteString(text)
		output = append(output, item.snapshot(status, true))
	}
	if choice.Message.ReasoningContent != "" {
		appendText("reasoning", "rs_", stripDSMLLeaks(choice.Message.ReasoningContent))
	}
	text := stripDSMLLeaks(rawText(choice.Message.Content))
	if text != "" || len(choice.Message.ToolCalls) == 0 {
		appendText("message", "msg_", text)
	}
	for i, call := range choice.Message.ToolCalls {
		item := &streamOutput{
			id: fmt.Sprintf("fc_%s_%d", strings.TrimPrefix(id, "resp_"), i), typ: "function_call",
			callID: call.ID, name: call.Function.Name,
		}
		item.text.WriteString(call.Function.Arguments)
		output = append(output, item.snapshot(status, true))
	}
	var input, completion, total, cached, reasoning int
	if u := src.Usage; u != nil {
		input, completion, total = u.PromptTokens, u.CompletionTokens, u.TotalTokens
		cached, reasoning = u.PromptTokensDetails.CachedTokens, u.CompletionTokensDetails.ReasoningTokens
	}
	if total == 0 {
		total = input + completion
	}
	out := map[string]any{
		"id": id, "object": "response", "created_at": src.Created,
		"status": status, "model": src.Model, "output": output, "error": nil, "incomplete_details": nil,
		"usage": map[string]any{
			"input_tokens": input, "output_tokens": completion, "total_tokens": total,
			"input_tokens_details":  map[string]any{"cached_tokens": cached},
			"output_tokens_details": map[string]any{"reasoning_tokens": reasoning},
		},
	}
	if reason != "" {
		out["incomplete_details"] = map[string]any{"reason": reason}
	}
	return json.Marshal(out)
}

func chatToAnthropic(body []byte) ([]byte, error) {
	var src chatCompletion
	if err := json.Unmarshal(body, &src); err != nil {
		return nil, err
	}
	text, _, finish := src.firstText()
	text = stripDSMLLeaks(text)
	stop := "end_turn"
	switch finish {
	case "length":
		stop = "max_tokens"
	case "tool_calls", "function_call":
		stop = "tool_use"
	case "content_filter":
		stop = "stop_sequence"
	}
	id := src.ID
	if id == "" {
		id = "msg_harbor"
	}
	out := map[string]any{
		"id":          id,
		"type":        "message",
		"role":        "assistant",
		"model":       src.Model,
		"content":     []any{map[string]any{"type": "text", "text": text}},
		"stop_reason": stop,
		"usage": map[string]any{
			"input_tokens":  src.Usage.PromptTokens,
			"output_tokens": src.Usage.CompletionTokens,
		},
	}
	return json.Marshal(out)
}

type chatCompletion struct {
	ID      string `json:"id"`
	Created int64  `json:"created"`
	Model   string `json:"model"`
	Choices []struct {
		FinishReason string `json:"finish_reason"`
		Message      struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"message"`
		Delta struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"delta"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
		TotalTokens      int `json:"total_tokens"`
	} `json:"usage"`
}

func (c chatCompletion) firstText() (text, role, finish string) {
	if len(c.Choices) == 0 {
		return "", "assistant", ""
	}
	ch := c.Choices[0]
	role = ch.Message.Role
	finish = ch.FinishReason
	text = rawText(ch.Message.Content)
	if text == "" {
		text = rawText(ch.Delta.Content)
	}
	if role == "" {
		role = ch.Delta.Role
	}
	return text, role, finish
}

func rawText(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return ""
	}
	if raw[0] == '"' {
		var s string
		if json.Unmarshal(raw, &s) == nil {
			return s
		}
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &parts) == nil {
		var b strings.Builder
		for _, p := range parts {
			if p.Type == "" || p.Type == "text" || p.Type == "output_text" {
				b.WriteString(p.Text)
			}
		}
		return b.String()
	}
	return ""
}

// dmlTagPattern 匹配 DeepSeek 系上游在超长 agent/thinking 任务下泄漏到
// content 的私有 DSL 标签（如 `</ | D$ML | parameter>` / `<| D$ML | invoke>`）。
// 这些是模型内部工具调用模板的残留，落到用户可见正文属于异常，Harbor 输出层
// 统一剥离（reasoning 块不剥，保持思考原文；正文只剥 message 分支）。
// D$ML 前后允许任意空白/管道符/斜杠与属性文本混合（覆盖 `</ | D$ML …>`、
// `<| /D$ML|parameter>` 等闭合变体）；普通正文极少含 `<D$ML…>`，误伤概率可忽略。
var dmlTagPattern = regexp.MustCompile(`(?i)<\s*[|/\s]*D\$ML[|/\s]*[^>]*>`)

// stripDSMLLeaks 从可见正文中剥离 DeepSeek D$ML 泄漏标签。这些标签对用户
// 无意义，是模型在超长上下文中把内部 DSL 吐进 content 的退化产物。
func stripDSMLLeaks(text string) string {
	if text == "" || !strings.Contains(strings.ToLower(text), "d$ml") {
		return text
	}
	return dmlTagPattern.ReplaceAllString(text, "")
}

func responseID(chatID string) string {
	switch {
	case strings.HasPrefix(chatID, "chatcmpl-"):
		return "resp_" + strings.TrimPrefix(chatID, "chatcmpl-")
	case strings.HasPrefix(chatID, "resp_"):
		return chatID
	case chatID != "":
		return "resp_" + chatID
	default:
		return "resp_harbor"
	}
}

func decodeObject(body []byte) (map[string]json.RawMessage, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("invalid json object: %w", err)
	}
	if raw == nil {
		raw = map[string]json.RawMessage{}
	}
	return raw, nil
}

func encodeObject(raw map[string]json.RawMessage) ([]byte, error) {
	out, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func decodeString(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return ""
}

func anthropicSystemText(raw json.RawMessage) string {
	if s := decodeString(raw); s != "" {
		return s
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &parts) != nil {
		return ""
	}
	var b strings.Builder
	for _, p := range parts {
		if p.Type == "" || p.Type == "text" {
			b.WriteString(p.Text)
		}
	}
	return b.String()
}

func normalizeAnthropicMessage(msg any) any {
	obj, ok := msg.(map[string]any)
	if !ok {
		return msg
	}
	content, ok := obj["content"]
	if !ok {
		return msg
	}
	switch c := content.(type) {
	case string:
		return msg
	case []any:
		texts := make([]string, 0, len(c))
		onlyText := true
		for _, part := range c {
			m, ok := part.(map[string]any)
			if !ok {
				onlyText = false
				break
			}
			typ, _ := m["type"].(string)
			if typ != "" && typ != "text" {
				onlyText = false
				break
			}
			text, _ := m["text"].(string)
			texts = append(texts, text)
		}
		if onlyText {
			obj["content"] = strings.Join(texts, "")
		}
	}
	return obj
}
