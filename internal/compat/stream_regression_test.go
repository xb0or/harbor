package compat

import (
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"harbor/internal/provider"
)

type regressionReadStep struct {
	data string
	err  error
}

type regressionStreamSource struct {
	steps    []regressionReadStep
	reads    int
	closed   bool
	closeErr error
	usage    provider.Usage
	known    bool
}

func (s *regressionStreamSource) Read(p []byte) (int, error) {
	s.reads++
	if len(s.steps) == 0 {
		return 0, io.EOF
	}
	step := &s.steps[0]
	n := copy(p, step.data)
	step.data = step.data[n:]
	if step.data != "" {
		return n, nil
	}
	err := step.err
	s.steps = s.steps[1:]
	return n, err
}

func (s *regressionStreamSource) Close() error {
	s.closed = true
	return s.closeErr
}

func (s *regressionStreamSource) StreamUsage() (provider.Usage, bool) {
	return s.usage, s.known
}

func regressionFrame(data string) string { return "data: " + data + "\n\n" }

func regressionFragments(data string, size int) []regressionReadStep {
	var steps []regressionReadStep
	for len(data) > 0 {
		n := min(size, len(data))
		steps = append(steps, regressionReadStep{data: data[:n]})
		data = data[n:]
	}
	return steps
}

func regressionEvents(t *testing.T, raw []byte) []map[string]any {
	t.Helper()
	var events []map[string]any
	for _, frame := range strings.Split(string(raw), "\n\n") {
		if frame == "" || frame == "data: [DONE]" {
			continue
		}
		lines := strings.Split(frame, "\n")
		if len(lines) != 2 || !strings.HasPrefix(lines[0], "event: ") || !strings.HasPrefix(lines[1], "data: ") {
			t.Fatalf("invalid SSE frame: %q", frame)
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(strings.TrimPrefix(lines[1], "data: ")), &event); err != nil {
			t.Fatal(err)
		}
		if event["type"] != strings.TrimPrefix(lines[0], "event: ") {
			t.Fatalf("event name does not match payload: %s", frame)
		}
		events = append(events, event)
	}
	return events
}

func regressionEvent(t *testing.T, events []map[string]any, typ string) map[string]any {
	t.Helper()
	for _, event := range events {
		if event["type"] == typ {
			return event
		}
	}
	t.Fatalf("missing %s in %v", typ, events)
	return nil
}

func regressionSequence(t *testing.T, events []map[string]any) {
	t.Helper()
	for i, event := range events {
		if event["sequence_number"] != float64(i) {
			t.Fatalf("sequence at %d = %v", i, event)
		}
	}
}

func TestStreamRegressionTailUsageAndLifecycle(t *testing.T) {
	src := &regressionStreamSource{steps: []regressionReadStep{
		{data: regressionFrame(`{"id":"chatcmpl-tail","model":"deepseek","created":42,"choices":[{"delta":{"role":"assistant","content":"hello "}}]}`)},
		{data: regressionFrame(`{"choices":[{"delta":{"content":"世界"}}]}`)},
		{data: regressionFrame(`{"choices":[{"delta":{},"finish_reason":"stop"}]}`)},
		{data: regressionFrame(`{"choices":[],"usage":{"prompt_tokens":7,"completion_tokens":4,"total_tokens":11,"prompt_tokens_details":{"cached_tokens":3},"completion_tokens_details":{"reasoning_tokens":2}}}`)},
		{data: regressionFrame(`[DONE]`)},
	}}
	bridge := wrapChatStream(KindResponses, src)
	raw, err := io.ReadAll(bridge)
	if err != nil {
		t.Fatal(err)
	}
	events := regressionEvents(t, raw)
	completed := regressionEvent(t, events, "response.completed")["response"].(map[string]any)
	usage := completed["usage"].(map[string]any)
	if usage["total_tokens"] != float64(11) {
		t.Fatalf("lost usage-only tail: %v; reads=%d", usage, src.reads)
	}
	if usage["input_tokens_details"].(map[string]any)["cached_tokens"] != float64(3) || usage["output_tokens_details"].(map[string]any)["reasoning_tokens"] != float64(2) {
		t.Fatalf("lost usage details: %v", usage)
	}
	if u, known := bridge.(provider.UsageReader).StreamUsage(); !known || u.TotalTokens != 11 {
		t.Fatalf("usage=%v known=%v", u, known)
	}
	var types []string
	for _, event := range events {
		types = append(types, event["type"].(string))
	}
	want := []string{"response.created", "response.in_progress", "response.output_item.added", "response.content_part.added", "response.output_text.delta", "response.output_text.delta", "response.output_text.done", "response.content_part.done", "response.output_item.done", "response.completed"}
	if !reflect.DeepEqual(types, want) {
		t.Fatalf("event sequence=%v want=%v", types, want)
	}
	regressionSequence(t, events)
	output := completed["output"].([]any)
	item := output[0].(map[string]any)
	if len(output) != 1 || item["status"] != "completed" || item["content"].([]any)[0].(map[string]any)["text"] != "hello 世界" {
		t.Fatalf("output=%v", output)
	}
	for _, event := range events {
		switch event["type"] {
		case "response.output_text.delta", "response.output_text.done", "response.content_part.added", "response.content_part.done":
			if event["item_id"] != item["id"] || event["output_index"] != float64(0) || event["content_index"] != float64(0) {
				t.Fatalf("missing text addressing: %v", event)
			}
		}
	}
	if !reflect.DeepEqual(regressionEvent(t, events, "response.output_item.done")["item"], item) {
		t.Fatalf("done item and final output differ: %s", raw)
	}
	if completed["id"] != "resp_tail" || completed["created_at"] != float64(42) || strings.Count(string(raw), "data: [DONE]") != 1 {
		t.Fatalf("bad terminal response: %s", raw)
	}
}

func TestStreamRegressionReasoningAndInterleavedTools(t *testing.T) {
	data := regressionFrame(`{"id":"chatcmpl-tools","choices":[{"delta":{"reasoning_content":"think "}}]}`) +
		regressionFrame(`{"choices":[{"delta":{"reasoning_content":"twice"}}]}`) +
		regressionFrame(`{"choices":[{"delta":{"tool_calls":[{"index":1,"id":"call_b","type":"function","function":{"name":"second","arguments":"{\"b\":"}},{"index":0,"id":"call_a","type":"function","function":{"name":"first","arguments":"{\"a\":"}}]}}]}`) +
		regressionFrame(`{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"1}"}},{"index":1,"function":{"arguments":"2}"}}]}}]}`) +
		regressionFrame(`{"choices":[{"delta":{"content":"ready"},"finish_reason":"tool_calls"}]}`) + regressionFrame(`[DONE]`)
	raw, err := io.ReadAll(wrapChatStream(KindResponses, &regressionStreamSource{steps: regressionFragments(data, 13)}))
	if err != nil {
		t.Fatal(err)
	}
	events := regressionEvents(t, raw)
	output := regressionEvent(t, events, "response.completed")["response"].(map[string]any)["output"].([]any)
	if len(output) != 4 {
		t.Fatalf("lost reasoning/tools: output=%v", output)
	}
	regressionSequence(t, events)
	reasoning := output[0].(map[string]any)
	if reasoning["type"] != "reasoning" || reasoning["summary"].([]any)[0].(map[string]any)["text"] != "think twice" {
		t.Fatalf("reasoning=%v", reasoning)
	}
	regressionEvent(t, events, "response.reasoning_summary_part.added")
	regressionEvent(t, events, "response.reasoning_summary_part.done")
	if regressionEvent(t, events, "response.reasoning_summary_text.done")["text"] != "think twice" {
		t.Fatal("reasoning summary done lost text")
	}
	added := make(map[string]bool)
	done := make(map[string]bool)
	arguments := make(map[string]string)
	argumentDone := make(map[string]bool)
	var reasoningText string
	for _, event := range events {
		typ := event["type"].(string)
		if typ == "response.output_item.added" || typ == "response.output_item.done" {
			item := event["item"].(map[string]any)
			id := item["id"].(string)
			index := int(event["output_index"].(float64))
			if output[index].(map[string]any)["id"] != id {
				t.Fatalf("unstable output index: %v", event)
			}
			if typ == "response.output_item.added" {
				if added[id] {
					t.Fatalf("duplicate item: %s", id)
				}
				added[id] = true
			} else {
				if !added[id] || done[id] {
					t.Fatalf("bad done lifecycle: %v", event)
				}
				done[id] = true
				if !reflect.DeepEqual(item, output[index]) {
					t.Fatalf("done output differs: %v", event)
				}
			}
		}
		if typ == "response.function_call_arguments.delta" || typ == "response.function_call_arguments.done" || strings.HasPrefix(typ, "response.reasoning_summary_") {
			id, ok := event["item_id"].(string)
			index, indexed := event["output_index"].(float64)
			if !ok || !indexed || !added[id] || done[id] || output[int(index)].(map[string]any)["id"] != id {
				t.Fatalf("bad delta addressing/lifecycle: %v", event)
			}
			if strings.HasPrefix(typ, "response.reasoning_summary_") && event["summary_index"] != float64(0) {
				t.Fatalf("bad summary index: %v", event)
			}
			if typ == "response.function_call_arguments.delta" {
				arguments[id] += event["delta"].(string)
			}
			if typ == "response.reasoning_summary_text.delta" {
				reasoningText += event["delta"].(string)
			}
			if typ == "response.function_call_arguments.done" {
				if argumentDone[id] || event["arguments"] != arguments[id] || event["name"] != output[int(index)].(map[string]any)["name"] {
					t.Fatalf("bad arguments done: %v", event)
				}
				argumentDone[id] = true
			}
		}
	}
	for i, want := range []struct{ callID, name, args string }{{"call_b", "second", `{"b":2}`}, {"call_a", "first", `{"a":1}`}} {
		item := output[i+1].(map[string]any)
		if item["type"] != "function_call" || item["call_id"] != want.callID || item["name"] != want.name || item["arguments"] != want.args || arguments[item["id"].(string)] != want.args {
			t.Fatalf("tool[%d]=%v deltas=%v", i, item, arguments)
		}
	}
	if len(added) != 4 || len(done) != 4 || len(argumentDone) != 2 || reasoningText != "think twice" {
		t.Fatalf("lifecycle added=%v done=%v argumentDone=%v reasoning=%q", added, done, argumentDone, reasoningText)
	}
}

func TestStreamRegressionIncompleteAndCleanEOF(t *testing.T) {
	for _, tc := range []struct{ finish, status, reason string }{{"stop", "completed", ""}, {"length", "incomplete", "max_output_tokens"}, {"content_filter", "incomplete", "content_filter"}} {
		for _, sentinel := range []bool{false, true} {
			t.Run(tc.finish+"/done="+map[bool]string{false: "false", true: "true"}[sentinel], func(t *testing.T) {
				data := regressionFrame(`{"choices":[{"delta":{"content":"partial"},"finish_reason":"` + tc.finish + `"}]}`)
				if sentinel {
					data += regressionFrame(`[DONE]`)
				}
				raw, err := io.ReadAll(wrapChatStream(KindResponses, io.NopCloser(strings.NewReader(data))))
				if err != nil {
					t.Fatal(err)
				}
				events := regressionEvents(t, raw)
				response := regressionEvent(t, events, "response."+tc.status)["response"].(map[string]any)
				if response["status"] != tc.status {
					t.Fatalf("response=%v", response)
				}
				if tc.reason != "" && response["incomplete_details"].(map[string]any)["reason"] != tc.reason {
					t.Fatalf("response=%v", response)
				}
				item := response["output"].([]any)[0].(map[string]any)
				if item["status"] != tc.status {
					t.Fatalf("item status=%v", item)
				}
			})
		}
	}
}

func TestStreamRegressionFailuresAreSanitized(t *testing.T) {
	secret := "PRIVATE_PROMPT_AND_KEY"
	readErr := errors.New(secret)
	text := regressionFrame(`{"choices":[{"delta":{"content":"partial"}}]}`)
	finish := regressionFrame(`{"choices":[{"delta":{},"finish_reason":"stop"}]}`)
	for _, tc := range []struct {
		name  string
		steps []regressionReadStep
		cause error
	}{
		{"empty_eof", nil, io.ErrUnexpectedEOF},
		{"unexpected_eof", []regressionReadStep{{data: text}}, io.ErrUnexpectedEOF},
		{"data_and_eof", []regressionReadStep{{data: text, err: io.EOF}}, io.ErrUnexpectedEOF},
		{"read_error", []regressionReadStep{{err: readErr}}, readErr},
		{"data_and_error", []regressionReadStep{{data: text, err: readErr}}, readErr},
		{"finish_and_error", []regressionReadStep{{data: text + finish, err: readErr}}, readErr},
		{"done_and_error", []regressionReadStep{{data: text + finish + regressionFrame(`[DONE]`), err: readErr}}, readErr},
		{"error_payload", []regressionReadStep{{data: text + regressionFrame(`{"error":{"message":"`+secret+`","type":"invalid_api_key"}}`) + regressionFrame(`[DONE]`)}}, nil},
		{"error_event", []regressionReadStep{{data: "event: error\ndata: {\"message\":\"" + secret + "\"}\n\n" + regressionFrame(`[DONE]`)}}, nil},
		{"error_after_finish", []regressionReadStep{{data: text + finish}, {data: regressionFrame(`{"error":"` + secret + `"}`)}}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := io.ReadAll(wrapChatStream(KindResponses, &regressionStreamSource{steps: tc.steps}))
			if err == nil || err == io.EOF {
				t.Fatalf("failure became success: %s", raw)
			}
			if tc.cause != nil && !errors.Is(err, tc.cause) {
				t.Fatalf("lost cause: err=%v want=%v", err, tc.cause)
			}
			if strings.Contains(string(raw), secret) || strings.Contains(err.Error(), secret) {
				t.Fatalf("unsanitized error: %s %v", raw, err)
			}
			if strings.Contains(string(raw), "response.completed") || strings.Contains(string(raw), "data: [DONE]") {
				t.Fatalf("false success: %s", raw)
			}
			events := regressionEvents(t, raw)
			response := regressionEvent(t, events, "response.failed")["response"].(map[string]any)
			if response["status"] != "failed" || response["error"].(map[string]any)["code"] != "upstream_error" {
				t.Fatalf("failure=%v", response)
			}
			regressionSequence(t, events)
		})
	}
}

func TestStreamRegressionToleratesMalformedFrames(t *testing.T) {
	// 单条非预期控制载荷只跳过，不熔断整条流：后续正常帧仍必须送达。
	data := ": heartbeat\n\n" +
		regressionFrame(`not-json`) +
		regressionFrame(`":{"broken":`) +
		regressionFrame(`{"choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]}`) +
		regressionFrame(`[DONE]`)
	raw, err := io.ReadAll(wrapChatStream(KindResponses, &regressionStreamSource{steps: regressionFragments(data, 7)}))
	if err != nil {
		t.Fatalf("malformed frame must not fail the stream: %v", err)
	}
	text := string(raw)
	if strings.Contains(text, "response.failed") || !strings.Contains(text, "response.completed") {
		t.Fatalf("stream did not recover: %s", text)
	}
	if !strings.Contains(text, `"delta":"ok"`) {
		t.Fatalf("valid frame after malformed frame was lost: %s", text)
	}
}

func TestStreamRegressionTerminalAndUsageVariants(t *testing.T) {
	text := regressionFrame(`{"id":"chatcmpl-eof","choices":[{"delta":{"content":"ok"}}]}`)
	finish := regressionFrame(`{"choices":[{"delta":{},"finish_reason":"stop"}]}`)
	for _, tc := range []struct {
		name  string
		steps []regressionReadStep
		known bool
		total int
	}{
		{"done_without_finish", []regressionReadStep{{data: text + regressionFrame(`[DONE]`), err: io.EOF}}, false, 0},
		{"finish_and_eof", []regressionReadStep{{data: text + finish, err: io.EOF}}, false, 0},
		{"zero_tail_usage", []regressionReadStep{{data: text + finish}, {data: regressionFrame(`{"choices":[],"usage":{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}}`) + regressionFrame(`[DONE]`)}}, true, 0},
		{"tail_usage_at_eof", []regressionReadStep{{data: text + finish}, {data: `data: {"usage":{"prompt_tokens":3,"completion_tokens":2}}`, err: io.EOF}}, true, 5},
		{"ignore_after_done", []regressionReadStep{{data: text + regressionFrame(`[DONE]`) + regressionFrame(`{"choices":[{"delta":{"content":"must_not_appear"}}]}`)}}, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bridge := wrapChatStream(KindResponses, &regressionStreamSource{steps: tc.steps})
			raw, err := io.ReadAll(bridge)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Count(string(raw), "event: response.completed") != 1 || strings.Contains(string(raw), "must_not_appear") {
				t.Fatalf("terminal stream: %s", raw)
			}
			usage, known := bridge.(provider.UsageReader).StreamUsage()
			if known != tc.known || usage.TotalTokens != tc.total {
				t.Fatalf("usage=%v known=%v", usage, known)
			}
			regressionSequence(t, regressionEvents(t, raw))
		})
	}
}

func TestStreamRegressionCRLFAndMultiline(t *testing.T) {
	data := ": heartbeat\r\nevent: message\r\ndata: {\"id\":\"chatcmpl-crlf\",\r\ndata: \"choices\":[{\"delta\":{\"content\":\"你好\"}}]}\r\n\r\n" +
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\r\n\r\ndata: [DONE]"
	raw, err := io.ReadAll(wrapChatStream(KindResponses, &regressionStreamSource{steps: regressionFragments(data, 1)}))
	if err != nil {
		t.Fatal(err)
	}
	events := regressionEvents(t, raw)
	if regressionEvent(t, events, "response.output_text.done")["text"] != "你好" {
		t.Fatalf("text lost: %s", raw)
	}
	regressionSequence(t, events)
}

func TestStreamRegressionBoundedLineAndEvent(t *testing.T) {
	for _, data := range []string{
		"data: " + strings.Repeat("x", (4<<20)+1),
		strings.Repeat("data: "+strings.Repeat("x", 1024)+"\n", 4097),
	} {
		src := &regressionStreamSource{steps: regressionFragments(data, 4096)}
		bridge := wrapChatStream(KindResponses, src).(*streamBridge)
		raw, err := io.ReadAll(bridge)
		if err == nil || !strings.Contains(string(raw), "response.failed") {
			t.Fatalf("oversized SSE accepted: err=%v bytes=%d", err, len(raw))
		}
		if len(bridge.pending) > 4<<20 {
			t.Fatalf("unbounded pending line: %d", len(bridge.pending))
		}
	}
}

func TestStreamRegressionCloseUsageAndZeroRead(t *testing.T) {
	closeErr := errors.New("close failed")
	src := &regressionStreamSource{closeErr: closeErr, usage: provider.Usage{PromptTokens: 3, CompletionTokens: 2, TotalTokens: 5}, known: true}
	bridge := wrapChatStream(KindResponses, src)
	if n, err := bridge.Read(nil); n != 0 || err != nil || src.reads != 0 {
		t.Fatalf("zero read consumed upstream: n=%d err=%v reads=%d", n, err, src.reads)
	}
	if usage, known := bridge.(provider.UsageReader).StreamUsage(); !known || usage != src.usage {
		t.Fatalf("usage=%v known=%v", usage, known)
	}
	if err := bridge.Close(); err != closeErr || !src.closed {
		t.Fatalf("Close did not delegate: err=%v closed=%v", err, src.closed)
	}
	if wrapChatStream(KindNone, src) != src || wrapChatStream(KindResponses, nil) != nil {
		t.Fatal("passthrough changed")
	}
}

func TestStreamRegressionAnthropicTextPath(t *testing.T) {
	data := regressionFrame(`{"id":"chatcmpl-anthropic","model":"m","choices":[{"delta":{"content":"hello "}}]}`) +
		regressionFrame(`{"choices":[{"delta":{"content":"world"},"finish_reason":"stop"}]}`) +
		regressionFrame(`{"choices":[],"usage":{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5}}`) + regressionFrame(`[DONE]`)
	src := &regressionStreamSource{steps: regressionFragments(data, 9)}
	bridge := wrapChatStream(KindAnthropic, src)
	raw, err := io.ReadAll(bridge)
	if err != nil {
		t.Fatal(err)
	}
	events := regressionEvents(t, raw)
	var types []string
	var text string
	for _, event := range events {
		types = append(types, event["type"].(string))
		if event["type"] == "content_block_delta" {
			text += event["delta"].(map[string]any)["text"].(string)
		}
	}
	want := []string{"message_start", "content_block_start", "content_block_delta", "content_block_delta", "content_block_stop", "message_delta", "message_stop"}
	if !reflect.DeepEqual(types, want) || text != "hello world" {
		t.Fatalf("events=%v text=%s", types, text)
	}
	delta := regressionEvent(t, events, "message_delta")
	if delta["delta"].(map[string]any)["stop_reason"] != "end_turn" || delta["usage"].(map[string]any)["output_tokens"] != float64(3) {
		t.Fatalf("message delta=%v", delta)
	}
	if usage, known := bridge.(provider.UsageReader).StreamUsage(); !known || usage.TotalTokens != 5 {
		t.Fatalf("usage=%v known=%v", usage, known)
	}
}

func TestStreamRegressionReasoningStripsDML(t *testing.T) {
	// 流式 reasoning：上游把 DeepSeek D$ML 标签泄进 reasoning_content，
	// Harbor 必须同样剥离（思考块也不残留 </ | D$ML | …> 尾巴）。
	data := regressionFrame(`{"id":"chatcmpl-dml-stream","choices":[{"delta":{"reasoning_content":"plan <| D$ML | invoke>"}}]}`) +
		regressionFrame(`{"choices":[{"delta":{"reasoning_content":"then </ | D$ML | parameter> act"},"finish_reason":"stop"}]}`) +
		regressionFrame(`[DONE]`)
	raw, err := io.ReadAll(wrapChatStream(KindResponses, &regressionStreamSource{steps: regressionFragments(data, 13)}))
	if err != nil {
		t.Fatal(err)
	}
	events := regressionEvents(t, raw)
	output := regressionEvent(t, events, "response.completed")["response"].(map[string]any)["output"].([]any)
	reasoning := output[0].(map[string]any)
	text := reasoning["summary"].([]any)[0].(map[string]any)["text"].(string)
	if strings.Contains(text, "D$ML") {
		t.Fatalf("reasoning must be stripped of D$ML, got %q", text)
	}
	if !strings.Contains(text, "plan") || !strings.Contains(text, "act") {
		t.Fatalf("reasoning lost real text, got %q", text)
	}
	if got := regressionEvent(t, events, "response.reasoning_summary_text.done")["text"].(string); strings.Contains(got, "D$ML") {
		t.Fatalf("reasoning summary done must be stripped, got %q", got)
	}
}
