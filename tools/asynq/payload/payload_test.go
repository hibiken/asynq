package payload

import (
	"encoding/base64"
	"strings"
	"strconv"
	"testing"
)

// b64 is a test helper that base64-encodes s (mimics how Go marshals []byte in JSON).
func b64(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}

func TestDecode(t *testing.T) {
	jsonInner := `{"abc":1}`
	textInner := "hello"
	bigNumJSON := `{"id":9223372036854775807}` // int64 max; must not become float

	tests := []struct {
		name   string
		data   []byte
		indent bool
		kind   string
		// payload substring checks (checked via strings.Contains in caller)
		wantHas    string
		wantNotHas string
		// envelope expectations
		wantEnv       bool
		wantEnvFields map[string]string // sampled/trace_id/span_id/reply_to/version/meta.<k>
	}{
		{
			name: "envelope with json inner payload (indent)",
			data: []byte(`{"_v":1,"_smpl":false,"payload":"` + b64(jsonInner) + `"}`),
			indent: true,
			kind:   KindEnvelope,
			wantHas: "\"abc\": 1",
			wantEnv: true,
			wantEnvFields: map[string]string{"version": "1", "sampled": "false"},
		},
		{
			name: "envelope with json inner payload (compact)",
			data: []byte(`{"_v":1,"_smpl":false,"payload":"` + b64(jsonInner) + `"}`),
			indent: false,
			kind:   KindEnvelope,
			wantHas: "\"abc\":1",
			wantNotHas: "\n",
			wantEnv: true,
		},
		{
			name: "envelope with text inner payload",
			data: []byte(`{"_v":1,"_smpl":true,"payload":"` + b64(textInner) + `"}`),
			indent: true,
			kind:   KindEnvelope,
			wantHas: "hello",
			wantEnv: true,
			wantEnvFields: map[string]string{"sampled": "true"},
		},
		{
			name: "envelope with meta and trace",
			data: []byte(`{"_v":1,"_smpl":true,"_tid":"t1","_sid":"s1","_meta":{"source":"quick-producer"},"payload":"` + b64(jsonInner) + `"}`),
			indent: true,
			kind:   KindEnvelope,
			wantHas: "\"abc\": 1",
			wantEnv: true,
			wantEnvFields: map[string]string{"trace_id": "t1", "span_id": "s1", "meta.source": "quick-producer"},
		},
		{
			name: "envelope preserves big int (no float corruption)",
			data: []byte(`{"_v":1,"_smpl":false,"payload":"` + b64(bigNumJSON) + `"}`),
			indent: true,
			kind:   KindEnvelope,
			wantHas: "9223372036854775807",
			wantNotHas: "9.223372036854776e+18",
			wantEnv: true,
		},
		{
			name: "plain json indent",
			data: []byte(`{"a":1,"b":2}`),
			indent: true,
			kind:   KindJSON,
			wantHas: "\"a\": 1",
		},
		{
			name: "plain json compact has no newline",
			data: []byte(`{"a":1,"b":2}`),
			indent: false,
			kind:   KindJSON,
			wantHas: "\"a\":1",
			wantNotHas: "\n",
		},
		{
			name: "whole base64 of json",
			data: []byte(b64(`{"k":"v"}`)),
			indent: true,
			kind:   KindBase64,
			wantHas: "\"k\": \"v\"",
		},
		{
			name: "plain text",
			data: []byte("hello world"),
			indent: true,
			kind:   KindText,
			wantHas: "hello world",
		},
		{
			name: "binary non-printable",
			data: []byte{0x00, 0x01, 0x02},
			indent: true,
			kind:   KindBinary,
			wantHas: "non-printable",
		},
		{
			name: "empty",
			data: nil,
			indent: true,
			kind: KindText,
		},
		{
			name: "not envelope: has _v/_smpl but no base64 payload -> json",
			data: []byte(`{"_v":1,"_smpl":false,"foo":"bar"}`),
			indent: true,
			kind:   KindJSON,
			wantHas: "\"foo\":",
		},
		{
			name: "not envelope: plain json with a payload string field but no _v",
			data: []byte(`{"payload":"` + b64(jsonInner) + `"}`),
			indent: true,
			kind:   KindJSON,
			wantEnv: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Decode(tc.data, tc.indent)
			if got.Kind != tc.kind {
				t.Fatalf("Kind = %q, want %q", got.Kind, tc.kind)
			}
			if tc.wantHas != "" && !strings.Contains(got.Payload, tc.wantHas) {
				t.Errorf("Payload = %q, want substring %q", got.Payload, tc.wantHas)
			}
			if tc.wantNotHas != "" && strings.Contains(got.Payload, tc.wantNotHas) {
				t.Errorf("Payload = %q, unexpectedly contains %q", got.Payload, tc.wantNotHas)
			}
			if tc.wantEnv != (got.Envelope != nil) {
				t.Fatalf("Envelope present = %v, want %v", got.Envelope != nil, tc.wantEnv)
			}
			if tc.wantEnv {
				for k, want := range tc.wantEnvFields {
					if got := envField(got.Envelope, k); got != want {
						t.Errorf("envelope %s = %q, want %q", k, got, want)
					}
				}
			}
		})
	}
}

func envField(e *Envelope, key string) string {
	switch key {
	case "version":
		return strconv.Itoa(e.Version)
	case "trace_id":
		return e.TraceID
	case "span_id":
		return e.SpanID
	case "sampled":
		return strconv.FormatBool(e.Sampled)
	case "reply_to":
		return e.ReplyTo
	}
	if k, ok := strings.CutPrefix(key, "meta."); ok {
		return e.Meta[k]
	}
	return ""
}

func TestFormatReturnsPayloadText(t *testing.T) {
	in := []byte(`{"_v":1,"_smpl":false,"payload":"` + b64(`{"x":2}`) + `"}`)
	got := Format(in, true)
	if !strings.Contains(got, "\"x\": 2") {
		t.Errorf("Format = %q, want decoded inner payload", got)
	}
}
