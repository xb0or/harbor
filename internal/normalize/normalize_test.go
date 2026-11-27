package normalize

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestBodyMessageIsString(t *testing.T) {
	raw := Body(ErrUpstream)
	var wire struct {
		Error struct {
			Message json.RawMessage `json:"message"`
			Type    string          `json:"type"`
			Code    string          `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatalf("body=%s err=%v", raw, err)
	}
	if len(wire.Error.Message) == 0 || wire.Error.Message[0] != '"' {
		t.Fatalf("error.message must be a JSON string, got %s", raw)
	}
	if wire.Error.Code != "upstream_error" || wire.Error.Type != "upstream_error" {
		t.Fatalf("body=%s", raw)
	}
	if strings.Contains(string(raw), "[object Object]") {
		t.Fatalf("body contains [object Object]: %s", raw)
	}
}

func TestStreamDataHasTopLevelMessage(t *testing.T) {
	raw := StreamData(ErrUpstream)
	var wire struct {
		Error struct {
			Message string `json:"message"`
			Code    string `json:"code"`
		} `json:"error"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatalf("body=%s err=%v", raw, err)
	}
	if wire.Message != "Upstream service error" {
		t.Fatalf("top-level message=%q body=%s", wire.Message, raw)
	}
	if wire.Error.Message != wire.Message || wire.Error.Code != "upstream_error" {
		t.Fatalf("body=%s", raw)
	}
}

func TestBodyNilFallsBack(t *testing.T) {
	raw := Body(nil)
	if !strings.Contains(string(raw), `"internal_error"`) {
		t.Fatalf("nil error body=%s", raw)
	}
}

func TestBodyWithRequestIDCarriesID(t *testing.T) {
	raw := BodyWithRequestID(ErrUpstream, "hr_abc123")
	if !strings.Contains(string(raw), `"request_id":"hr_abc123"`) {
		t.Fatalf("body missing request_id: %s", raw)
	}
	// 不带 ID 时不出该字段（保持纯 Body 行为）。
	if strings.Contains(string(Body(ErrUpstream)), `"request_id"`) {
		t.Fatalf("Body() without id leaked request_id: %s", Body(ErrUpstream))
	}
}

func TestStreamDataWithRequestIDCarriesID(t *testing.T) {
	raw := StreamDataWithRequestID(ErrUpstream, "hr_abc456")
	if !strings.Contains(string(raw), `"request_id":"hr_abc456"`) {
		t.Fatalf("stream data missing request_id: %s", raw)
	}
	if strings.Contains(string(StreamData(ErrUpstream)), `"request_id"`) {
		t.Fatalf("StreamData() without id leaked request_id: %s", StreamData(ErrUpstream))
	}
}

func TestParseUpstreamErrorExtractsCodeTypeMessage(t *testing.T) {
	body := `{"error":{"message":"This model's maximum context length is 8192 tokens","type":"invalid_request_error","code":"context_length_exceeded"}}`
	code, typ, msg := ParseUpstreamError(body)
	if code != "context_length_exceeded" || typ != "invalid_request_error" || !strings.Contains(msg, "maximum context length") {
		t.Fatalf("ParseUpstreamError = %q %q %q", code, typ, msg)
	}
	// 数字 code（部分上游返回数字）也应保留。
	code, _, _ = ParseUpstreamError(`{"error":{"code":400,"message":"bad"}}`)
	if code != "400" {
		t.Fatalf("numeric code = %q, want 400", code)
	}
	// 非错误体 / 非法 JSON 返回空，不 panic。
	if c, tt, m := ParseUpstreamError(`not json`); c != "" || tt != "" || m != "" {
		t.Fatalf("invalid body parsed as %q %q %q", c, tt, m)
	}
	if c, tt, m := ParseUpstreamError(``); c != "" || tt != "" || m != "" {
		t.Fatalf("empty body parsed as %q %q %q", c, tt, m)
	}
}

func TestUpstreamKeepsRawDetailsInternallyOnly(t *testing.T) {
	e := Upstream("provider-x", 400, `{"error":{"message":"bad param: max_tokens too large","type":"invalid_request_error","code":"invalid_request"}}`)
	if e.UpstreamCode != "invalid_request" || e.UpstreamType != "invalid_request_error" || !strings.Contains(e.UpstreamMessage, "max_tokens") {
		t.Fatalf("upstream raw details not captured: %+v", e)
	}
	// 原始详情（上游 message 内容）绝不出现在对外 body / stream / error() 字符串。
	// Reason 含 provider 名是内部日志设计，不属于对外泄露。
	for name, raw := range map[string]string{
		"body":   string(Body(e)),
		"stream": string(StreamData(e)),
		"err":    e.Error(),
	} {
		if strings.Contains(raw, "max_tokens") || strings.Contains(raw, "too large") {
			t.Fatalf("%s leaked upstream raw message: %s", name, raw)
		}
	}
}
