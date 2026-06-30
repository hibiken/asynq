// Package payload decodes and pretty-prints asynq task payloads for display.
//
// It handles:
//   - nsp-style trace envelopes ({"_v":1,...,"payload":"<base64>"}) — the inner
//     payload is base64-decoded and pretty-printed, with envelope metadata exposed.
//   - whole-input base64 strings (decoded; if the result is JSON it is pretty-printed).
//   - plain JSON (re-indented).
//   - printable text (returned as-is).
//   - non-printable binary (described as a placeholder).
//
// The package does not depend on any nsp-platform code; envelopes are detected
// purely by the "_v"/"_smpl"/"payload" field shape plus a base64-decodable payload.
package payload

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Result kinds.
const (
	KindEnvelope = "envelope"
	KindBase64   = "base64"
	KindJSON     = "json"
	KindText     = "text"
	KindBinary   = "binary"
)

// Result describes a decoded asynq task payload.
type Result struct {
	// Kind classifies the input (one of the Kind* constants).
	Kind string

	// Payload is the decoded, human-readable business payload text.
	// For JSON input it is re-indented (multi-line when indent=true, compact
	// single-line when indent=false). For binary input it is a placeholder.
	Payload string

	// Envelope is non-nil only when Kind == KindEnvelope.
	Envelope *Envelope
}

// Envelope holds transport/trace metadata extracted from an nsp-style envelope.
type Envelope struct {
	Version    int
	TraceID    string            // _tid
	SpanID     string            // _sid
	Sampled    bool              // _smpl
	HasSampled bool              // whether _smpl was present
	ReplyTo    string            // _rto (raw JSON string, for display)
	Meta       map[string]string // _meta
}

// base64Re matches the standard base64 alphabet (used only as a charset check).
var base64Re = regexp.MustCompile(`^[A-Za-z0-9+/]+={0,2}$`)

// Decode decodes and pretty-prints a payload, returning a structured result
// (including envelope metadata when present). Used by dash modal and task inspect.
func Decode(data []byte, indent bool) Result {
	if len(data) == 0 {
		return Result{Kind: KindText, Payload: ""}
	}
	if env, inner, ok := tryEnvelope(data, indent); ok {
		return Result{Kind: KindEnvelope, Payload: inner, Envelope: env}
	}
	if dec, ok := tryBase64(data); ok {
		return Result{Kind: KindBase64, Payload: prettyOrRaw(dec, indent)}
	}
	if json.Valid(data) {
		return Result{Kind: KindJSON, Payload: reindentJSON(data, indent)}
	}
	if isPrintable(data) {
		return Result{Kind: KindText, Payload: string(data)}
	}
	return Result{Kind: KindBinary, Payload: fmt.Sprintf("<non-printable, %d bytes>", len(data))}
}

// Format is a convenience wrapper returning only the decoded payload text.
// Used by task list table cells (pass indent=false for single-line output).
func Format(data []byte, indent bool) string {
	return Decode(data, indent).Payload
}

// tryEnvelope detects an nsp-style envelope: valid JSON object with _v (int),
// _smpl (present), and a payload field that is a base64 string. Returns the
// extracted envelope metadata and the decoded/pretty-printed inner payload.
func tryEnvelope(data []byte, indent bool) (*Envelope, string, bool) {
	if !json.Valid(data) {
		return nil, "", false
	}
	var probe struct {
		Version *int             `json:"_v"`
		Sampled *bool            `json:"_smpl"`
		Payload *json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return nil, "", false
	}
	if probe.Version == nil || *probe.Version < 1 || probe.Sampled == nil || probe.Payload == nil {
		return nil, "", false
	}
	var payloadStr string
	if err := json.Unmarshal(*probe.Payload, &payloadStr); err != nil {
		return nil, "", false
	}
	dec, err := base64.StdEncoding.DecodeString(payloadStr)
	if err != nil {
		return nil, "", false
	}
	return extractEnvelope(data), prettyOrRaw(dec, indent), true
}

func extractEnvelope(data []byte) *Envelope {
	var env struct {
		Version int               `json:"_v"`
		TraceID string            `json:"_tid"`
		SpanID  string            `json:"_sid"`
		Sampled *bool             `json:"_smpl"`
		ReplyTo *json.RawMessage  `json:"_rto"`
		Meta    map[string]string `json:"_meta"`
	}
	_ = json.Unmarshal(data, &env) // data is known valid JSON
	out := &Envelope{
		Version: env.Version,
		TraceID: env.TraceID,
		SpanID:  env.SpanID,
		Meta:    env.Meta,
	}
	if env.Sampled != nil {
		out.Sampled = *env.Sampled
		out.HasSampled = true
	}
	if env.ReplyTo != nil {
		out.ReplyTo = string(*env.ReplyTo)
	}
	return out
}

// tryBase64 reports whether data is a whole base64 string whose decoded bytes
// are meaningful text or JSON. The checks (charset, length, utf8 validity,
// printability) reduce false positives on ordinary text.
func tryBase64(data []byte) ([]byte, bool) {
	s := strings.TrimSpace(string(data))
	if len(s) < 4 || len(s)%4 != 0 || !base64Re.MatchString(s) {
		return nil, false
	}
	dec, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, false
	}
	if !utf8.Valid(dec) {
		return nil, false
	}
	if json.Valid(dec) || isPrintable(dec) {
		return dec, true
	}
	return nil, false
}

// prettyOrRaw formats decoded bytes: re-indented if JSON, as-is if printable
// text, otherwise a binary placeholder.
func prettyOrRaw(b []byte, indent bool) string {
	if json.Valid(b) {
		return reindentJSON(b, indent)
	}
	if isPrintable(b) {
		return string(b)
	}
	return fmt.Sprintf("<non-printable, %d bytes>", len(b))
}

// reindentJSON re-formats JSON whitespace using json.Indent/json.Compact, which
// preserve key order and exact numeric values (no float64 round-trip).
func reindentJSON(data []byte, indent bool) string {
	var buf bytes.Buffer
	if indent {
		if err := json.Indent(&buf, data, "", "  "); err != nil {
			return string(data)
		}
	} else {
		if err := json.Compact(&buf, data); err != nil {
			return string(data)
		}
	}
	return buf.String()
}

// isPrintable reports whether data is human-readable text (matches dash's semantics).
func isPrintable(data []byte) bool {
	if !utf8.Valid(data) {
		return false
	}
	isAllSpace := true
	for _, r := range string(data) {
		if !unicode.IsGraphic(r) {
			return false
		}
		if !unicode.IsSpace(r) {
			isAllSpace = false
		}
	}
	return !isAllSpace
}
