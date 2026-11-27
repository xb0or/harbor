// 思考字段兼容层回归测试：非流式补字段，流式必须原样透传。
package compat

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"harbor/internal/provider"
)

// TestAliasReasoningBodyKeepsNumbersAndAddsContent 验证非流式响应补齐字段且不改动数字/其它字段。
func TestAliasReasoningBodyKeepsNumbersAndAddsContent(t *testing.T) {
	body := []byte(`{"id":"c1","created":1789043399,"model":"m","choices":[{"index":0,"message":{"role":"assistant","reasoning":"想一下","content":"答案"},"finish_reason":"stop"}]}`)
	got := AliasReasoningBody(body)
	for _, want := range []string{`"reasoning_content":"想一下"`, `"created":1789043399`, `"content":"答案"`, `"model":"m"`} {
		if !bytes.Contains(got, []byte(want)) {
			t.Fatalf("missing %s in %s", want, got)
		}
	}
	if bytes.Contains(got, []byte("e+09")) {
		t.Fatalf("number was rewritten: %s", got)
	}
}

// TestAliasReasoningBodyWithoutReasoningIsUntouched 没有思考字段时字节不变。
func TestAliasReasoningBodyWithoutReasoningIsUntouched(t *testing.T) {
	body := []byte(`{"choices":[{"index":0,"delta":{"content":"hi"}}]}`)
	if got := AliasReasoningBody(body); !bytes.Equal(got, body) {
		t.Fatalf("body changed unexpectedly: %s", got)
	}
	already := []byte(`{"choices":[{"index":0,"delta":{"reasoning":"x","reasoning_content":"x"}}]}`)
	if got := AliasReasoningBody(already); !bytes.Equal(got, already) {
		t.Fatalf("already aliased body changed: %s", got)
	}
}

// memoryStream 是最小流式响应体，用来确认 ApplyResponse 的包装行为。
type memoryStream struct{ src *strings.Reader }

func (m *memoryStream) Read(p []byte) (int, error) { return m.src.Read(p) }
func (m *memoryStream) Close() error               { return nil }

// TestApplyResponseNormalizesStreamReasoning 流式同协议只做思考字段标准化：
// reasoning → reasoning_content，删除冗余 reasoning，并保留 "content": null。
func TestApplyResponseNormalizesStreamReasoning(t *testing.T) {
	raw := `data: {"choices":[{"index":0,"delta":{"role":"assistant","reasoning":"第一步"}}]}` + "\n\n" +
		"data: [DONE]\n\n"
	resp := &provider.Response{Stream: true, StreamBody: &memoryStream{src: strings.NewReader(raw)}}
	if err := ApplyResponse(KindNone, resp); err != nil {
		t.Fatalf("apply response: %v", err)
	}
	out, err := io.ReadAll(resp.StreamBody)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	text := string(out)
	if strings.Contains(text, `"reasoning":`) {
		t.Fatalf("redundant reasoning field must be removed: %s", text)
	}
	if !strings.Contains(text, `"reasoning_content":"第一步"`) {
		t.Fatalf("missing reasoning_content: %s", text)
	}
	if !strings.Contains(text, `"content":null`) {
		t.Fatalf("thinking delta must keep content:null: %s", text)
	}
	if !strings.Contains(text, "data: [DONE]") {
		t.Fatalf("terminal frame lost: %s", text)
	}
}

// TestApplyResponseStreamReframesOrdinaryLines 无思考字段时内容不变，
// 但每个完整行都补齐事件边界（对齐 agent2api：不依赖上游空行）。
func TestApplyResponseStreamReframesOrdinaryLines(t *testing.T) {
	raw := "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n" +
		": comment\n\n" +
		"data: not-json\n\n" +
		"data: [DONE]\n\n"
	want := "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n" +
		": comment\n\n" +
		"data: not-json\n\n" +
		"data: [DONE]\n\n"
	resp := &provider.Response{Stream: true, StreamBody: &memoryStream{src: strings.NewReader(raw)}}
	if err := ApplyResponse(KindNone, resp); err != nil {
		t.Fatalf("apply response: %v", err)
	}
	out, err := io.ReadAll(resp.StreamBody)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if string(out) != want {
		t.Fatalf("ordinary frames not reframed:\n want %q\n got  %q", want, string(out))
	}
}
