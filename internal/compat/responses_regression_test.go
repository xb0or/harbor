package compat

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestResponsesRequestToolRoundTrip(t *testing.T) {
	_, body, _, err := RewriteRequest("/v1/responses", []byte(`{"model":"m","stream":true,"stream_options":{"include_obfuscation":true},
		"tools":[{"type":"function","name":"lookup","description":"Find a value","parameters":{"type":"object","properties":{}},"strict":false}],
		"tool_choice":{"type":"function","name":"lookup"},"parallel_tool_calls":true,
		"reasoning":{"effort":"low"},"text":{"format":{"type":"json_schema","name":"result","strict":true,"schema":{"type":"object"}}},
		"input":[
			{"role":"developer","content":"Be brief"},
			{"role":"user","content":[{"type":"input_text","text":"look up both"}]},
			{"type":"reasoning","summary":[{"type":"summary_text","text":"Need two values"}]},
			{"type":"function_call","call_id":"call_a","name":"lookup","arguments":"{}"},
			{"type":"function_call","call_id":"call_b","name":"lookup","arguments":"{}"},
			{"type":"function_call_output","call_id":"call_a","output":[{"type":"input_text","text":"A"}]},
			{"type":"function_call_output","call_id":"call_b","output":"B"}
		]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Tools []struct {
			Type     string
			Function struct {
				Name, Description string
				Strict            *bool
				Parameters        map[string]any
			}
		}
		ToolChoice struct {
			Type     string
			Function struct{ Name string }
		} `json:"tool_choice"`
		StreamOptions   map[string]any `json:"stream_options"`
		ReasoningEffort string         `json:"reasoning_effort"`
		ResponseFormat  map[string]any `json:"response_format"`
		Messages        []struct {
			Role             string
			Content          any
			ToolCalls        []json.RawMessage `json:"tool_calls"`
			ReasoningContent string            `json:"reasoning_content"`
		}
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Tools) != 1 || got.Tools[0].Function.Name != "lookup" || got.Tools[0].Function.Strict == nil || *got.Tools[0].Function.Strict {
		t.Errorf("function definition not converted: %s", body)
	}
	if got.ToolChoice.Function.Name != "lookup" {
		t.Errorf("tool_choice not converted: %s", body)
	}
	if len(got.StreamOptions) != 1 || got.StreamOptions["include_usage"] != true {
		t.Errorf("stream_options=%v", got.StreamOptions)
	}
	if got.ReasoningEffort != "low" {
		t.Errorf("reasoning effort lost: %s", body)
	}
	if got.ResponseFormat["type"] != "json_schema" {
		t.Errorf("structured output lost: %s", body)
	}
	if len(got.Messages) != 5 {
		t.Fatalf("parallel calls must share one assistant turn: %s", body)
	}
	if got.Messages[0].Role != "system" {
		t.Errorf("developer role not bridged: %s", body)
	}
	if len(got.Messages[2].ToolCalls) != 2 || got.Messages[2].ReasoningContent != "Need two values" {
		t.Errorf("tool turn lost: %s", body)
	}
	if got.Messages[3].Content != "A" || got.Messages[4].Content != "B" {
		t.Errorf("tool outputs lost: %s", body)
	}
}

func TestResponsesRequestRejectsUnsupportedFeatures(t *testing.T) {
	for _, body := range []string{
		`{"model":"m","input":"hi","tools":[{"type":"web_search_preview"}]}`,
		`{"model":"m","input":"hi","tools":[{"type":"function"}]}`,
		`{"model":"m","previous_response_id":"resp_old","input":"continue"}`,
		`{"model":"m","input":[{"type":"item_reference","id":"msg_old"}]}`,
	} {
		if _, _, _, err := RewriteRequest("/v1/responses", []byte(body)); err == nil {
			t.Errorf("silently accepted unsupported request: %s", body)
		}
	}
}

func TestResponsesRequestKeepsNestedFunctionsAndSyncOptions(t *testing.T) {
	_, body, _, err := RewriteRequest("/v1/responses", []byte(`{"model":"m","input":"hi","stream":false,"stream_options":{"include_usage":true},"tools":[{"type":"function","function":{"name":"f","parameters":{"type":"object"}}}],"tool_choice":"auto"}`))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if _, ok := got["stream_options"]; ok {
		t.Errorf("sync request contains stream_options: %s", body)
	}
	tool := got["tools"].([]any)[0].(map[string]any)
	if tool["function"].(map[string]any)["name"] != "f" || got["tool_choice"] != "auto" {
		t.Errorf("nested function changed: %s", body)
	}
}

func TestResponsesSynchronousToolsAndReasoning(t *testing.T) {
	out, err := chatToResponses([]byte(`{"id":"chatcmpl-tools","model":"m","choices":[{"message":{"role":"assistant","content":null,"reasoning_content":"Need a lookup","tool_calls":[{"id":"call_x","type":"function","function":{"name":"lookup","arguments":"{\"x\":1}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":10,"completion_tokens":5,"prompt_tokens_details":{"cached_tokens":3},"completion_tokens_details":{"reasoning_tokens":2}}}`))
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Output []map[string]any
		Usage  map[string]any
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Output) != 2 || got.Output[0]["type"] != "reasoning" {
		t.Fatalf("reasoning/tool output lost: %s", out)
	}
	call := got.Output[1]
	if call["type"] != "function_call" || call["call_id"] != "call_x" || call["name"] != "lookup" || call["arguments"] != `{"x":1}` {
		t.Errorf("tool output=%v", call)
	}
	if got.Usage["total_tokens"] != float64(15) || got.Usage["input_tokens_details"].(map[string]any)["cached_tokens"] != float64(3) || got.Usage["output_tokens_details"].(map[string]any)["reasoning_tokens"] != float64(2) {
		t.Errorf("usage=%v", got.Usage)
	}
}

func TestResponsesSynchronousIncomplete(t *testing.T) {
	for _, reason := range []string{"length", "content_filter"} {
		out, err := chatToResponses([]byte(`{"choices":[{"message":{"role":"assistant","content":"partial"},"finish_reason":"` + reason + `"}]}`))
		if err != nil {
			t.Fatal(err)
		}
		var got struct {
			Status            string
			IncompleteDetails map[string]any `json:"incomplete_details"`
		}
		if err := json.Unmarshal(out, &got); err != nil {
			t.Fatal(err)
		}
		if got.Status != "incomplete" || got.IncompleteDetails["reason"] == nil {
			t.Errorf("incomplete reason lost: %s", out)
		}
	}
}

// DeepSeek 系上游不认 OpenAI 风格 reasoning_effort（会 400 invalid_request），
// 必须转成 DeepSeek thinking 参数；非 DeepSeek 模型保持 reasoning_effort 不变。
func TestResponsesRequestDeepSeekThinkingDialect(t *testing.T) {
	cases := []struct {
		name         string
		model        string
		effort       string
		wantThinking string
		wantEffort   bool
	}{
		{"deepseek max", "deepseek-v4-flash-0731", "max", `{"type":"enabled","budget_tokens":8192}`, false},
		{"deepseek low", "deepseek-chat", "low", `{"type":"enabled","budget_tokens":1024}`, false},
		{"deepseek none", "deepseek-v4", "none", "", false},
		{"deepseek empty", "deepseek-chat", "", "", false},
		{"openai high", "gpt-4o", "high", "", true},
		{"openai max", "gpt-5", "max", "", true},
		{"mock keeps effort", "m", "low", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, body, _, err := RewriteRequest("/v1/responses", []byte(`{"model":"`+tc.model+`","input":"hi","reasoning":{"effort":"`+tc.effort+`"}}`))
			if err != nil {
				t.Fatal(err)
			}
			var got struct {
				Thinking        map[string]any `json:"thinking"`
				ReasoningEffort string         `json:"reasoning_effort"`
			}
			if err := json.Unmarshal(body, &got); err != nil {
				t.Fatal(err)
			}
			if tc.wantThinking != "" {
				if got.Thinking == nil || got.Thinking["type"] != "enabled" || got.Thinking["budget_tokens"] == nil {
					t.Fatalf("missing thinking param: %s", body)
				}
				wantBudget := json.RawMessage(tc.wantThinking)
				var want map[string]any
				if err := json.Unmarshal(wantBudget, &want); err != nil {
					t.Fatal(err)
				}
				if got.Thinking["type"] != want["type"] || got.Thinking["budget_tokens"] != want["budget_tokens"] {
					t.Errorf("thinking=%v want %v", got.Thinking, want)
				}
				if got.ReasoningEffort != "" {
					t.Errorf("deepseek must not carry reasoning_effort: %s", body)
				}
			} else {
				if got.Thinking != nil {
					t.Errorf("unexpected thinking param: %s", body)
				}
				if tc.wantEffort && got.ReasoningEffort != tc.effort {
					t.Errorf("reasoning_effort=%q want %q", got.ReasoningEffort, tc.effort)
				}
				if !tc.wantEffort && got.ReasoningEffort != "" {
					t.Errorf("none/empty must clear reasoning_effort: %s", body)
				}
			}
		})
	}
}

func TestStripDSMLLeaks(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"plain text untouched", "你好，世界", "你好，世界"},
		{"close parameter tag", "结果如下：</ | D$ML | parameter>", "结果如下："},
		{"open invoke tag", "<| D$ML | invoke> 开始执行", " 开始执行"},
		{"lowercase variant", "正文<| d$ml | parameter>尾部", "正文尾部"},
		{"multiple tags", "a</ | D$ML | parameter>b<| D$ML | invoke>c", "abc"},
		{"reasoning text preserved", "plan call get_weather</ | D$ML | invoke>", "plan call get_weather"},
		{"surrounding content kept", "先查询天气<| D$ML | parameter>city<| /D$ML|parameter>再回答", "先查询天气city再回答"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := stripDSMLLeaks(tc.in); got != tc.want {
				t.Errorf("stripDSMLLeaks(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestResponsesNonStreamStripsDMLFromContent(t *testing.T) {
	// 非流式 responses 输出：上游把 D$ML 泄进 message.content，Harbor 必须剥离，
	// 同时 reasoning 原文保留。
	body := []byte(`{"id":"chatcmpl-dml","model":"m","choices":[{"message":{"role":"assistant",
		"content":"结果：</ | D$ML | parameter> 查询成功",
		"reasoning_content":"init plan <| D$ML | invoke>"},"finish_reason":"stop"}],
		"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}`)
	out, err := chatToResponses(body)
	if err != nil {
		t.Fatal(err)
	}
	var resp struct {
		Output []struct {
			Type    string `json:"type"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
			Summary []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"summary"`
		} `json:"output"`
	}
	if err := json.Unmarshal(out, &resp); err != nil {
		t.Fatalf("unmarshal: %v body=%s", err, out)
	}
	var contentText, reasoningText string
	for _, item := range resp.Output {
		switch item.Type {
		case "message":
			for _, p := range item.Content {
				contentText += p.Text
			}
		case "reasoning":
			for _, s := range item.Summary {
				reasoningText += s.Text
			}
		}
	}
	if strings.Contains(contentText, "D$ML") {
		t.Errorf("content must be stripped of D$ML, got %q", contentText)
	}
	if !strings.Contains(contentText, "查询成功") {
		t.Errorf("content must keep real text, got %q", contentText)
	}
	if strings.Contains(reasoningText, "D$ML") {
		t.Errorf("reasoning text must also be stripped of D$ML, got %q", reasoningText)
	}
	if !strings.Contains(reasoningText, "init plan") {
		t.Errorf("reasoning text must keep real reasoning, got %q", reasoningText)
	}
}
