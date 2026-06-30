# Payload 解码与 dash 展示优化 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 asynq 任务的 payload 在 `asynq` CLI（`task ls` / `task inspect`）和 `asynq dash` TUI 中可读——解码 base64、识别 nsp 信封、JSON 美化打印，dash 中长 payload 支持弹窗内上下键滚动。

**Architecture:** 新增纯函数叶子包 `tools/asynq/payload`，封装「解码 + 美化」逻辑（信封识别 + 整段 base64 + JSON/文本/二进制），被 `cmd`（CLI）和 `cmd/dash`（TUI）共同复用，避免 `cmd → dash` 已有依赖造成的循环引用。dash 弹窗改为「整窗内容按 offset 滚动」，payload 解码后按宽度折行，envelope 元数据作为附加行展示。CLI `task ls --decode` 表格用单行解码（保持现状为默认），`task inspect --decode` 多行美化并补全当前缺失的 payload 字段。

**Tech Stack:** Go 1.22+（`tools/go.mod`），标准库 `encoding/json`、`encoding/base64`、`unicode/utf8`、`github.com/gdamore/tcell/v2`、`github.com/mattn/go-runewidth`、`github.com/spf13/cobra`。

**Spec:** `docs/superpowers/specs/2026-06-30-payload-decode-and-dash-view-design.md`

**Build / test 命令（全部在 `tools/` 模块下执行）：**
- 构建：`cd tools && go build ./...`
- 测试全部：`cd tools && go test ./...`
- 测试单包：`cd tools && go test ./asynq/payload/`、`cd tools && go test ./asynq/cmd/dash/`

**模块/包路径：**
- 新包：`github.com/hibiken/asynq/tools/asynq/payload`（文件在 `tools/asynq/payload/`）
- CLI 包：`github.com/hibiken/asynq/tools/asynq/cmd`（文件在 `tools/asynq/cmd/`）
- dash 包：`github.com/hibiken/asynq/tools/asynq/cmd/dash`（文件在 `tools/asynq/cmd/dash/`）

---

## Task 1: 新建 payload 解码包

**Files:**
- Create: `tools/asynq/payload/payload.go`
- Test: `tools/asynq/payload/payload_test.go`

- [ ] **Step 1: 写失败测试**

Create `tools/asynq/payload/payload_test.go`:

```go
package payload

import (
	"encoding/base64"
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
			if tc.wantHas != "" && !contains(got.Payload, tc.wantHas) {
				t.Errorf("Payload = %q, want substring %q", got.Payload, tc.wantHas)
			}
			if tc.wantNotHas != "" && contains(got.Payload, tc.wantNotHas) {
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

func contains(s, sub string) bool {
	return len(sub) == 0 || len(s) >= len(sub) && indexOf(s, sub) >= 0
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func envField(e *Envelope, key string) string {
	switch key {
	case "version":
		return itoa(e.Version)
	case "trace_id":
		return e.TraceID
	case "span_id":
		return e.SpanID
	case "sampled":
		return boolStr(e.Sampled)
	case "reply_to":
		return e.ReplyTo
	}
	return ""
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func TestFormatReturnsPayloadText(t *testing.T) {
	in := []byte(`{"_v":1,"_smpl":false,"payload":"` + b64(`{"x":2}`) + `"}`)
	got := Format(in, true)
	if !contains(got, "\"x\": 2") {
		t.Errorf("Format = %q, want decoded inner payload", got)
	}
}
```

Note: `strings.Contains`/`strconv`/`fmt` are intentionally avoided in the test's tiny helpers to keep the test self-contained, but you may simplify by importing `strings` and `strconv` and replacing `contains`/`itoa`/`boolStr` with `strings.Contains`, `strconv.Itoa`, `strconv.FormatBool`. If you do, delete the helper funcs.

- [ ] **Step 2: 运行测试确认失败**

Run: `cd tools && go test ./asynq/payload/`
Expected: FAIL / build error — `payload.go` does not exist yet (undefined: `Decode`, `Result`, `Envelope`, kind constants).

- [ ] **Step 3: 写实现**

Create `tools/asynq/payload/payload.go`:

```go
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
	TraceID    string // _tid
	SpanID     string // _sid
	Sampled    bool   // _smpl
	HasSampled bool   // whether _smpl was present
	ReplyTo    string // _rto (raw JSON string, for display)
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
	var buf strings.Builder
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
```

- [ ] **Step 4: 运行测试确认通过**

Run: `cd tools && go test ./asynq/payload/`
Expected: PASS (`ok github.com/hibiken/asynq/tools/asynq/payload ...`).

- [ ] **Step 5: 构建确认 + 提交**

Run: `cd tools && go build ./...`
Expected: builds with no errors.

```bash
git add tools/asynq/payload/payload.go tools/asynq/payload/payload_test.go
git commit -m "feat(tools): add payload decode package (envelope/base64/json)"
```

---

## Task 2: dash 纯函数辅助（折行 / 偏移夹紧 / 尺寸计算）

These are the testable primitives the scrollable modal depends on. We add them and refactor `withModal` to share the size math.

**Files:**
- Modify: `tools/asynq/cmd/dash/draw.go` (add helpers; refactor `withModal`)
- Test: `tools/asynq/cmd/dash/draw_test.go`

- [ ] **Step 1: 写失败测试**

Append to `tools/asynq/cmd/dash/draw_test.go` (keep the existing `TestTruncate`):

```go
func TestWrapLines(t *testing.T) {
	tests := []struct {
		s    string
		max  int
		want []string
	}{
		{"", 5, []string{""}},
		{"abc", 10, []string{"abc"}},
		{"abcdefghij", 4, []string{"abcd", "efgh", "ij"}},
		{"a\nb\nc", 10, []string{"a", "b", "c"}},
		{"line1\nline2", 4, []string{"line", "1", "line", "2"}},
	}
	for _, tc := range tests {
		got := wrapLines(tc.s, tc.max)
		if len(got) != len(tc.want) {
			t.Fatalf("wrapLines(%q,%d) = %v, want %v", tc.s, tc.max, got, tc.want)
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("wrapLines(%q,%d)[%d] = %q, want %q", tc.s, tc.max, i, got[i], tc.want[i])
			}
		}
	}
}

func TestClampMaxOffset(t *testing.T) {
	tests := []struct {
		total, height, want int
	}{
		{0, 10, 0},
		{5, 10, 0},  // fewer rows than height -> no scroll
		{10, 10, 0}, // exactly fits
		{12, 10, 2},
		{20, 0, 19}, // height<1 treated as 1
	}
	for _, tc := range tests {
		got := clampMaxOffset(tc.total, tc.height)
		if got != tc.want {
			t.Errorf("clampMaxOffset(%d,%d) = %d, want %d", tc.total, tc.height, got, tc.want)
		}
	}
}

func TestModalDimsFromSize(t *testing.T) {
	cw, bh := modalDimsFromSize(100, 50)
	// modalWidth=60 -> content=56; modalHeight=30 -> body=28
	if cw != 56 || bh != 28 {
		t.Errorf("modalDimsFromSize(100,50) = (%d,%d), want (56,28)", cw, bh)
	}
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cd tools && go test ./asynq/cmd/dash/`
Expected: FAIL — undefined `wrapLines`, `clampMaxOffset`, `modalDimsFromSize`.

- [ ] **Step 3: 加实现并重构 withModal**

In `tools/asynq/cmd/dash/draw.go`:

3a. Add `runewidth` is already imported. Add the three helper functions (place near `withModal`):

```go
// wrapLines splits s into visual lines no wider than max (measured by rune width).
// Existing newlines are honored; over-long logical lines are wrapped.
func wrapLines(s string, max int) []string {
	if max <= 0 {
		return []string{s}
	}
	var out []string
	for _, line := range strings.Split(s, "\n") {
		var cur strings.Builder
		curW := 0
		for _, r := range line {
			rw := runewidth.RuneWidth(r)
			if curW+rw > max && cur.Len() > 0 {
				out = append(out, cur.String())
				cur.Reset()
				curW = 0
			}
			cur.WriteRune(r)
			curW += rw
		}
		out = append(out, cur.String())
	}
	return out
}

// clampMaxOffset returns the largest scroll offset that still shows content,
// i.e. max(0, total-height). height<1 is treated as 1.
func clampMaxOffset(total, height int) int {
	if height < 1 {
		height = 1
	}
	if total <= height {
		return 0
	}
	return total - height
}

// modalDimsFromSize computes (contentWidth, bodyHeight) from screen size using
// the same proportions as withModal (60% width/height, 4-char horizontal padding,
// 2-char vertical border).
func modalDimsFromSize(w, h int) (contentWidth, bodyHeight int) {
	modalWidth := int(math.Floor(float64(w) * 0.6))
	modalHeight := int(math.Floor(float64(h) * 0.6))
	return modalWidth - 4, modalHeight - 2
}

func modalDims(s tcell.Screen) (contentWidth, bodyHeight int) {
	w, h := s.Size()
	return modalDimsFromSize(w, h)
}
```

3b. Refactor `withModal` to compute its dimensions via `modalDimsFromSize`. Replace the existing dimension block inside `withModal`:

Find this block in `withModal`:
```go
	var (
		modalWidth  = int(math.Floor(float64(w) * 0.6))
		modalHeight = int(math.Floor(float64(h) * 0.6))
		rowOffset   = int(math.Floor(float64(h) * 0.2)) // 20% from the top
		colOffset   = int(math.Floor(float64(w) * 0.2)) // 20% from the left
	)
	if modalHeight < 3 {
		return // no content can be shown
	}
```

Replace with:
```go
	modalWidth, modalHeight := modalDimsFromSize(w, h)
	modalWidth += 4  // restore full modal width (content + padding)
	modalHeight += 2 // restore full modal height (body + borders)
	rowOffset := int(math.Floor(float64(h) * 0.2)) // 20% from the top
	colOffset := int(math.Floor(float64(w) * 0.2)) // 20% from the left
	if modalHeight < 3 {
		return // no content can be shown
	}
```

(We restore `+4`/`+2` because `withModal` uses the *full* modal box for borders, while `modalDimsFromSize` returns the *content* area. This keeps `withModal`'s rendering byte-identical to before.)

- [ ] **Step 4: 运行测试确认通过**

Run: `cd tools && go test ./asynq/cmd/dash/`
Expected: PASS (existing `TestTruncate` + new tests all pass).

- [ ] **Step 5: 提交**

```bash
git add tools/asynq/cmd/dash/draw.go tools/asynq/cmd/dash/draw_test.go
git commit -m "feat(dash): add line-wrap/offset/modal-dims helpers, share size math"
```

---

## Task 3: dash 弹窗解码 + 整窗滚动渲染

Rewire `drawTaskModal` to decode the payload, wrap it to the content width, append envelope metadata rows, and window the full body by `state.payloadScrollOffset`.

**Files:**
- Modify: `tools/asynq/cmd/dash/draw.go` (`drawTaskModal`; add `buildModalRows`, `modalMaxOffset`)
- Modify: `tools/asynq/cmd/dash/dash.go` (add `payloadScrollOffset` to `State`)

- [ ] **Step 1: 给 State 加滚动偏移字段**

In `tools/asynq/cmd/dash/dash.go`, find the `State` struct and add the field after `pageNum int`:

```go
	pageNum int // pagination page number

	payloadScrollOffset int // scroll offset (body rows) for the task modal

	view     viewType // current view type
	prevView viewType // to support "go back"
```

- [ ] **Step 2: 改 drawTaskModal 为解码+滚动**

In `tools/asynq/cmd/dash/draw.go`, add the `payload` import to the import block:

```go
	"github.com/hibiken/asynq/tools/asynq/payload"
```

Then replace the entire `drawTaskModal` function with:

```go
func drawTaskModal(d *ScreenDrawer, state *State) {
	if state.taskID == "" {
		return
	}
	task := state.selectedTask
	if task == nil {
		// task no longer found
		withModal(d, []func(d *modalRowDrawer){
			func(d *modalRowDrawer) { d.Print("=== Task Info ===", baseStyle.Bold(true)) },
			func(d *modalRowDrawer) { d.Print("", baseStyle) },
			func(d *modalRowDrawer) {
				d.Print(fmt.Sprintf("Task %q no longer exists", state.taskID), baseStyle)
			},
		})
		return
	}

	contentWidth, bodyHeight := modalDims(d.Screen())
	rows := buildModalRows(task, contentWidth)

	offset := state.payloadScrollOffset
	if max := clampMaxOffset(len(rows), bodyHeight); offset > max {
		offset = max
	}
	if offset < 0 {
		offset = 0
	}
	end := offset + bodyHeight
	if end > len(rows) {
		end = len(rows)
	}
	withModal(d, rows[offset:end])
}

// buildModalRows builds the ordered body rows for the task modal (title, fields,
// decoded payload lines, envelope metadata, result). Width is used to wrap the
// payload. The caller windows the result by scroll offset.
func buildModalRows(task *asynq.TaskInfo, contentWidth int) []func(*modalRowDrawer) {
	res := payload.Decode(task.Payload, true)

	var rows []func(*modalRowDrawer)
	add := func(fn func(*modalRowDrawer)) { rows = append(rows, fn) }

	add(func(d *modalRowDrawer) { d.Print("=== Task Info ===", baseStyle.Bold(true)) })
	add(func(d *modalRowDrawer) { d.Print("", baseStyle) })
	add(func(d *modalRowDrawer) {
		d.Print("ID: ", labelStyle)
		d.Print(task.ID, baseStyle)
	})
	add(func(d *modalRowDrawer) {
		d.Print("Type: ", labelStyle)
		d.Print(task.Type, baseStyle)
	})
	add(func(d *modalRowDrawer) {
		d.Print("State: ", labelStyle)
		d.Print(task.State.String(), baseStyle)
	})
	add(func(d *modalRowDrawer) {
		d.Print("Queue: ", labelStyle)
		d.Print(task.Queue, baseStyle)
	})
	add(func(d *modalRowDrawer) {
		d.Print("Retry: ", labelStyle)
		d.Print(fmt.Sprintf("%d/%d", task.Retried, task.MaxRetry), baseStyle)
	})
	if task.LastErr != "" {
		add(func(d *modalRowDrawer) {
			d.Print("Last Failure: ", labelStyle)
			d.Print(task.LastErr, baseStyle)
		})
		add(func(d *modalRowDrawer) {
			d.Print("Last Failure Time: ", labelStyle)
			d.Print(fmt.Sprintf("%v (%s)", task.LastFailedAt, formatPastTime(task.LastFailedAt)), baseStyle)
		})
	}
	if !task.NextProcessAt.IsZero() {
		add(func(d *modalRowDrawer) {
			d.Print("Next Process Time: ", labelStyle)
			d.Print(fmt.Sprintf("%v (%s)", task.NextProcessAt, formatNextProcessTime(task.NextProcessAt)), baseStyle)
		})
	}
	if !task.CompletedAt.IsZero() {
		add(func(d *modalRowDrawer) {
			d.Print("Completion Time: ", labelStyle)
			d.Print(fmt.Sprintf("%v (%s)", task.CompletedAt, formatPastTime(task.CompletedAt)), baseStyle)
		})
	}

	// Payload (decoded, multi-line, wrapped to content width)
	add(func(d *modalRowDrawer) { d.Print("Payload:", labelStyle) })
	for _, line := range wrapLines(res.Payload, contentWidth-2) {
		line := line
		add(func(d *modalRowDrawer) { d.Print(line, baseStyle) })
	}

	// Envelope metadata
	if res.Envelope != nil {
		appendEnvelopeRows(add, res.Envelope)
	}

	// Result (decoded, multi-line, wrapped)
	if task.Result != nil {
		add(func(d *modalRowDrawer) { d.Print("Result:", labelStyle) })
		for _, line := range wrapLines(payload.Format(task.Result, true), contentWidth-2) {
			line := line
			add(func(d *modalRowDrawer) { d.Print(line, baseStyle) })
		}
	}

	return rows
}

// modalMaxOffset returns the maximum scroll offset for the task modal, given the
// current screen size and the selected task's content. Used by key handling.
func modalMaxOffset(s tcell.Screen, task *asynq.TaskInfo) int {
	contentWidth, bodyHeight := modalDims(s)
	rows := buildModalRows(task, contentWidth)
	return clampMaxOffset(len(rows), bodyHeight)
}

// appendEnvelopeRows adds envelope metadata rows to the modal body.
func appendEnvelopeRows(add func(func(*modalRowDrawer)), e *payload.Envelope) {
	add(func(d *modalRowDrawer) { d.Print("Envelope:", labelStyle) })
	add(func(d *modalRowDrawer) {
		d.Print(fmt.Sprintf("  version: %d", e.Version), baseStyle)
	})
	if e.HasSampled {
		add(func(d *modalRowDrawer) {
			d.Print(fmt.Sprintf("  sampled: %v", e.Sampled), baseStyle)
		})
	}
	if e.TraceID != "" {
		add(func(d *modalRowDrawer) { d.Print(fmt.Sprintf("  trace_id: %s", e.TraceID), baseStyle) })
	}
	if e.SpanID != "" {
		add(func(d *modalRowDrawer) { d.Print(fmt.Sprintf("  span_id: %s", e.SpanID), baseStyle) })
	}
	if e.ReplyTo != "" {
		add(func(d *modalRowDrawer) { d.Print(fmt.Sprintf("  reply_to: %s", e.ReplyTo), baseStyle) })
	}
	for k, v := range e.Meta {
		k, v := k, v
		add(func(d *modalRowDrawer) { d.Print(fmt.Sprintf("  meta.%s: %s", k, v), baseStyle) })
	}
}
```

- [ ] **Step 3: 构建确认（此时 key handling 还没接，但渲染要先能编译）**

Run: `cd tools && go build ./...`
Expected: builds cleanly. (`State.payloadScrollOffset` is read in drawTaskModal; not yet written anywhere, default 0 is fine.)

- [ ] **Step 4: 运行现有 dash 测试确认无回归**

Run: `cd tools && go test ./asynq/cmd/dash/`
Expected: PASS.

- [ ] **Step 5: 提交**

```bash
git add tools/asynq/cmd/dash/draw.go tools/asynq/cmd/dash/dash.go
git commit -m "feat(dash): decode payload, show envelope metadata, scrollable modal"
```

---

## Task 4: dash 键位（弹窗滚动 + offset 重置）+ footer/help 提示

Wire up/down/j/k to scroll the modal when it is open, reset the offset on task select / modal close, and surface hints in the footer and help screen.

**Files:**
- Modify: `tools/asynq/cmd/dash/key_event.go`
- Modify: `tools/asynq/cmd/dash/draw.go` (`drawFooter`, `drawHelp`)

- [ ] **Step 1: 接滚动键位**

In `tools/asynq/cmd/dash/key_event.go`:

1a. In `downKeyQueueDetails`, add an `else` branch for the modal-open case. Replace:

```go
	} else if state.taskID == "" {
		if state.taskTableRowIdx < len(state.tasks) {
			state.taskTableRowIdx++
		} else {
			state.taskTableRowIdx = 0 // loop back
		}
	}
	h.drawer.Draw(state)
```

with:

```go
	} else if state.taskID == "" {
		if state.taskTableRowIdx < len(state.tasks) {
			state.taskTableRowIdx++
		} else {
			state.taskTableRowIdx = 0 // loop back
		}
	} else {
		// modal open: scroll content down
		if state.selectedTask != nil && state.payloadScrollOffset < modalMaxOffset(s, state.selectedTask) {
			state.payloadScrollOffset++
		}
	}
	h.drawer.Draw(state)
```

1b. In `upKeyQueueDetails`, add the matching `else` branch. Replace:

```go
	} else if state.taskID == "" {
		if state.taskTableRowIdx == 0 {
			state.taskTableRowIdx = len(state.tasks)
		} else {
			state.taskTableRowIdx--
		}
	}
	h.drawer.Draw(state)
```

with:

```go
	} else if state.taskID == "" {
		if state.taskTableRowIdx == 0 {
			state.taskTableRowIdx = len(state.tasks)
		} else {
			state.taskTableRowIdx--
		}
	} else {
		// modal open: scroll content up
		if state.payloadScrollOffset > 0 {
			state.payloadScrollOffset--
		}
	}
	h.drawer.Draw(state)
```

- [ ] **Step 2: 选中 / 关闭弹窗时重置 offset**

2a. In `enterKeyQueueDetails`, after the line `state.taskID = task.ID` add a reset. The block currently is:

```go
	} else if !shouldShowGroupTable(state) && state.taskTableRowIdx != 0 {
		task := state.tasks[state.taskTableRowIdx-1]
		state.selectedTask = task
		state.taskID = task.ID
		f.Fetch(state)
```

Change to:

```go
	} else if !shouldShowGroupTable(state) && state.taskTableRowIdx != 0 {
		task := state.tasks[state.taskTableRowIdx-1]
		state.selectedTask = task
		state.taskID = task.ID
		state.payloadScrollOffset = 0
		f.Fetch(state)
```

2b. In `goBack`, reset offset when closing the modal. The branch currently is:

```go
		if state.taskID != "" {
			state.taskID = ""
			state.selectedTask = nil
			d.Draw(state)
		}
```

Change to:

```go
		if state.taskID != "" {
			state.taskID = ""
			state.selectedTask = nil
			state.payloadScrollOffset = 0
			d.Draw(state)
		}
```

- [ ] **Step 3: footer 提示**

In `tools/asynq/cmd/dash/draw.go`, replace the `switch state.view` block inside `drawFooter`:

```go
	switch state.view {
	case viewTypeHelp:
		d.Print("<Esc>: GoBack", style)
	default:
		d.Print("<?>: Help    <Ctrl+C>: Exit ", style)
	}
```

with:

```go
	switch state.view {
	case viewTypeHelp:
		d.Print("<Esc>: GoBack", style)
	case viewTypeQueueDetails:
		if state.taskID != "" {
			d.Print("<↑/↓> or <j/k>: Scroll    <Esc>/<q>: Back    <?>: Help ", style)
		} else {
			d.Print("<?>: Help    <Ctrl+C>: Exit ", style)
		}
	default:
		d.Print("<?>: Help    <Ctrl+C>: Exit ", style)
	}
```

(Note: `↑`/`↓` are the ↑/↓ arrows, kept as escapes to avoid non-ASCII in the source edit. You may replace them with literal `↑`/`↓` if your editor handles UTF-8.)

- [ ] **Step 4: help 屏幕补一行**

In `drawHelp` (in `draw.go`), add a row for modal scrolling. Insert this immediately before the `<Ctrl+C>` row func:

```go
		func(d *modalRowDrawer) {
			d.Print("<UpArrow>", keyStyle)
			d.Print("/", baseStyle)
			d.Print("<DownArrow>", keyStyle)
			d.Print(" scroll payload in task modal", baseStyle)
		},
```

- [ ] **Step 5: 构建并测试**

Run: `cd tools && go build ./... && go test ./asynq/cmd/dash/`
Expected: builds; dash tests PASS.

- [ ] **Step 6: 提交**

```bash
git add tools/asynq/cmd/dash/key_event.go tools/asynq/cmd/dash/draw.go
git commit -m "feat(dash): scroll task modal with up/down, reset on open/close"
```

---

## Task 5: dash 表格列解码

Decode payload (and Result) in the task tables using single-line output so columns stay aligned.

**Files:**
- Modify: `tools/asynq/cmd/dash/draw.go` (the `*TaskTableColumns` definitions)

- [ ] **Step 1: 替换表格列的 payload/result 渲染**

In `tools/asynq/cmd/dash/draw.go`, replace every occurrence of:

```go
func(t *asynq.TaskInfo) string { return formatByteSlice(t.Payload) }
```

with:

```go
func(t *asynq.TaskInfo) string { return payload.Format(t.Payload, false) }
```

There are 7 occurrences across `activeTaskTableColumns`, `pendingTaskTableColumns`, `aggregatingTaskTableColumns`, `scheduledTaskTableColumns`, `retryTaskTableColumns`, `archivedTaskTableColumns`, `completedTaskTableColumns`.

And in `completedTaskTableColumns` replace:

```go
{"Result", alignLeft, func(t *asynq.TaskInfo) string { return formatByteSlice(t.Result) }},
```

with:

```go
{"Result", alignLeft, func(t *asynq.TaskInfo) string { return payload.Format(t.Result, false) }},
```

(The `payload` import was added in Task 3, so no import change needed here. `formatByteSlice` becomes unused but compiles fine as a package-level func; leave it in place to keep the diff minimal.)

- [ ] **Step 2: 构建并测试**

Run: `cd tools && go build ./... && go test ./asynq/cmd/dash/`
Expected: builds; tests PASS. (If `go vet`/golangci-lint flags unused `formatByteSlice`, that is acceptable; do not delete it unless lint fails the build.)

- [ ] **Step 3: 提交**

```bash
git add tools/asynq/cmd/dash/draw.go
git commit -m "feat(dash): decode payload/result in task tables (single-line)"
```

---

## Task 6: CLI `task ls --decode` 与 `task inspect --decode`

Add a `--decode/-d` flag to `task list` and `task inspect`, thread it through, and add payload + envelope display to `printTaskInfo` (which currently omits payload).

**Files:**
- Modify: `tools/asynq/cmd/task.go`
- Modify: `tools/asynq/cmd/root.go` (add `formatPayload` helper)

- [ ] **Step 1: 加 formatPayload 便利函数**

In `tools/asynq/cmd/root.go`, add the `payload` import alongside the existing imports:

```go
	"github.com/hibiken/asynq/tools/asynq/payload"
```

Then, immediately after the existing `sprintBytes` function (around `root.go:471`), add:

```go
// formatPayload returns a display string for a task payload. When decode is true,
// the payload is decoded (base64/envelope) and pretty-printed as single-line JSON
// (suitable for a table cell). Otherwise it behaves like sprintBytes.
func formatPayload(p []byte, decode bool) string {
	if !decode {
		return sprintBytes(p)
	}
	return payload.Format(p, false)
}
```

- [ ] **Step 2: 给 task list / task inspect 加 --decode flag**

In `tools/asynq/cmd/task.go` `init()`, after the `taskListCmd.Flags().StringP("group", ...)` line, add:

```go
	taskListCmd.Flags().BoolP("decode", "d", false, "decode base64/envelope payloads and pretty-print (single-line)")
```

And after the `taskInspectCmd.MarkFlagRequired("id")` line, add:

```go
	taskInspectCmd.Flags().BoolP("decode", "d", false, "decode base64/envelope payload and pretty-print")
```

- [ ] **Step 3: taskList 读取 flag 并透传**

In `taskList` (in `task.go`), read the flag and pass it into each list function. Replace the body of `taskList` starting from where `pageSize` is read, i.e. replace:

```go
	pageSize, err := cmd.Flags().GetInt("size")
	if err != nil {
		return err
	}

	switch state {
	case "active":
		return listActiveTasks(qname, pageNum, pageSize)
	case "pending":
		return listPendingTasks(qname, pageNum, pageSize)
	case "scheduled":
		return listScheduledTasks(qname, pageNum, pageSize)
	case "retry":
		return listRetryTasks(qname, pageNum, pageSize)
	case "archived":
		return listArchivedTasks(qname, pageNum, pageSize)
	case "completed":
		return listCompletedTasks(qname, pageNum, pageSize)
	case "aggregating":
		group, err := cmd.Flags().GetString("group")
		if err != nil {
			return err
		}
		if group == "" {
			return fmt.Errorf("flag --group is required for listing aggregating tasks")
		}
		return listAggregatingTasks(qname, group, pageNum, pageSize)
	default:
		return fmt.Errorf("state=%q is not supported", state)
	}
```

with:

```go
	pageSize, err := cmd.Flags().GetInt("size")
	if err != nil {
		return err
	}
	decode, err := cmd.Flags().GetBool("decode")
	if err != nil {
		return err
	}

	switch state {
	case "active":
		return listActiveTasks(qname, pageNum, pageSize, decode)
	case "pending":
		return listPendingTasks(qname, pageNum, pageSize, decode)
	case "scheduled":
		return listScheduledTasks(qname, pageNum, pageSize, decode)
	case "retry":
		return listRetryTasks(qname, pageNum, pageSize, decode)
	case "archived":
		return listArchivedTasks(qname, pageNum, pageSize, decode)
	case "completed":
		return listCompletedTasks(qname, pageNum, pageSize, decode)
	case "aggregating":
		group, err := cmd.Flags().GetString("group")
		if err != nil {
			return err
		}
		if group == "" {
			return fmt.Errorf("flag --group is required for listing aggregating tasks")
		}
		return listAggregatingTasks(qname, group, pageNum, pageSize, decode)
	default:
		return fmt.Errorf("state=%q is not supported", state)
	}
```

- [ ] **Step 4: 改各 listXxxTasks 签名并用 formatPayload**

Update each of the seven list functions to accept `decode bool` and use `formatPayload`. For each function:

- Change signature, e.g. `func listActiveTasks(qname string, pageNum, pageSize int) error {` → `func listActiveTasks(qname string, pageNum, pageSize int, decode bool) error {`
- In the `fmt.Fprintf(w, tmpl, ...)` call, replace `sprintBytes(t.Payload)` with `formatPayload(t.Payload, decode)`.
- For `listCompletedTasks` also replace `sprintBytes(t.Result)` with `formatPayload(t.Result, decode)`.

The complete set of updated functions (replace each in full):

```go
func listActiveTasks(qname string, pageNum, pageSize int, decode bool) error {
	i := createInspector()
	tasks, err := i.ListActiveTask(qname, asynq.PageSize(pageSize), asynq.Page(pageNum))
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		fmt.Printf("No active tasks in %q queue\n", qname)
		return nil
	}
	printTable(
		[]string{"ID", "Type", "Payload"},
		func(w io.Writer, tmpl string) {
			for _, t := range tasks {
				fmt.Fprintf(w, tmpl, t.ID, t.Type, formatPayload(t.Payload, decode))
			}
		},
	)
	return nil
}

func listPendingTasks(qname string, pageNum, pageSize int, decode bool) error {
	i := createInspector()
	tasks, err := i.ListPendingTasks(qname, asynq.PageSize(pageSize), asynq.Page(pageNum))
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		fmt.Printf("No pending tasks in %q queue\n", qname)
		return nil
	}
	printTable(
		[]string{"ID", "Type", "Payload"},
		func(w io.Writer, tmpl string) {
			for _, t := range tasks {
				fmt.Fprintf(w, tmpl, t.ID, t.Type, formatPayload(t.Payload, decode))
			}
		},
	)
	return nil
}

func listScheduledTasks(qname string, pageNum, pageSize int, decode bool) error {
	i := createInspector()
	tasks, err := i.ListScheduledTasks(qname, asynq.PageSize(pageSize), asynq.Page(pageNum))
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		fmt.Printf("No scheduled tasks in %q queue\n", qname)
		return nil
	}
	printTable(
		[]string{"ID", "Type", "Payload", "Process In"},
		func(w io.Writer, tmpl string) {
			for _, t := range tasks {
				fmt.Fprintf(w, tmpl, t.ID, t.Type, formatPayload(t.Payload, decode), formatProcessAt(t.NextProcessAt))
			}
		},
	)
	return nil
}

func listRetryTasks(qname string, pageNum, pageSize int, decode bool) error {
	i := createInspector()
	tasks, err := i.ListRetryTasks(qname, asynq.PageSize(pageSize), asynq.Page(pageNum))
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		fmt.Printf("No retry tasks in %q queue\n", qname)
		return nil
	}
	printTable(
		[]string{"ID", "Type", "Payload", "Next Retry", "Last Error", "Last Failed", "Retried", "Max Retry"},
		func(w io.Writer, tmpl string) {
			for _, t := range tasks {
				fmt.Fprintf(w, tmpl, t.ID, t.Type, formatPayload(t.Payload, decode), formatProcessAt(t.NextProcessAt),
					t.LastErr, formatPastTime(t.LastFailedAt), t.Retried, t.MaxRetry)
			}
		},
	)
	return nil
}

func listArchivedTasks(qname string, pageNum, pageSize int, decode bool) error {
	i := createInspector()
	tasks, err := i.ListArchivedTasks(qname, asynq.PageSize(pageSize), asynq.Page(pageNum))
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		fmt.Printf("No archived tasks in %q queue\n", qname)
		return nil
	}
	printTable(
		[]string{"ID", "Type", "Payload", "Last Failed", "Last Error"},
		func(w io.Writer, tmpl string) {
			for _, t := range tasks {
				fmt.Fprintf(w, tmpl, t.ID, t.Type, formatPayload(t.Payload, decode), formatPastTime(t.LastFailedAt), t.LastErr)
			}
		})
	return nil
}

func listCompletedTasks(qname string, pageNum, pageSize int, decode bool) error {
	i := createInspector()
	tasks, err := i.ListCompletedTasks(qname, asynq.PageSize(pageSize), asynq.Page(pageNum))
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		fmt.Printf("No completed tasks in %q queue\n", qname)
		return nil
	}
	printTable(
		[]string{"ID", "Type", "Payload", "CompletedAt", "Result"},
		func(w io.Writer, tmpl string) {
			for _, t := range tasks {
				fmt.Fprintf(w, tmpl, t.ID, t.Type, formatPayload(t.Payload, decode), formatPastTime(t.CompletedAt), formatPayload(t.Result, decode))
			}
		})
	return nil
}

func listAggregatingTasks(qname, group string, pageNum, pageSize int, decode bool) error {
	i := createInspector()
	tasks, err := i.ListAggregatingTasks(qname, group, asynq.PageSize(pageSize), asynq.Page(pageNum))
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		fmt.Printf("No aggregating tasks in group %q \n", group)
		return nil
	}
	printTable(
		[]string{"ID", "Type", "Payload", "Group"},
		func(w io.Writer, tmpl string) {
			for _, t := range tasks {
				fmt.Fprintf(w, tmpl, t.ID, t.Type, formatPayload(t.Payload, decode), t.Group)
			}
		},
	)
	return nil
}
```

- [ ] **Step 5: taskInspect 读取 flag，printTaskInfo 展示 payload + envelope**

In `task.go`, replace `taskInspect`:

```go
func taskInspect(cmd *cobra.Command, args []string) error {
	qname, err := cmd.Flags().GetString("queue")
	if err != nil {
		return err
	}
	id, err := cmd.Flags().GetString("id")
	if err != nil {
		return err
	}
	decode, err := cmd.Flags().GetBool("decode")
	if err != nil {
		return err
	}

	i := createInspector()
	info, err := i.GetTaskInfo(qname, id)
	if err != nil {
		return fmt.Errorf("could not get task info: %v", err)
	}
	printTaskInfo(info, decode)
	return nil
}
```

And replace `printTaskInfo` with:

```go
func printTaskInfo(info *asynq.TaskInfo, decode bool) {
	bold := color.New(color.Bold)
	bold.Println("Task Info")
	fmt.Printf("Queue:   %s\n", info.Queue)
	fmt.Printf("ID:      %s\n", info.ID)
	fmt.Printf("Type:    %s\n", info.Type)
	fmt.Printf("State:   %v\n", info.State)
	fmt.Printf("Retried: %d/%d\n", info.Retried, info.MaxRetry)
	fmt.Println()
	fmt.Printf("Next process time: %s\n", formatNextProcessAt(info.NextProcessAt))
	if len(info.LastErr) != 0 {
		fmt.Println()
		bold.Println("Last Failure")
		fmt.Printf("Failed at:     %s\n", formatPastTime(info.LastFailedAt))
		fmt.Printf("Error message: %s\n", info.LastErr)
	}

	// Payload
	fmt.Println()
	bold.Println("Payload")
	if decode {
		res := payload.Decode(info.Payload, true)
		fmt.Println(res.Payload)
		if res.Envelope != nil {
			bold.Println("Envelope")
			fmt.Printf("  version: %d\n", res.Envelope.Version)
			if res.Envelope.HasSampled {
				fmt.Printf("  sampled: %v\n", res.Envelope.Sampled)
			}
			if res.Envelope.TraceID != "" {
				fmt.Printf("  trace_id: %s\n", res.Envelope.TraceID)
			}
			if res.Envelope.SpanID != "" {
				fmt.Printf("  span_id: %s\n", res.Envelope.SpanID)
			}
			if res.Envelope.ReplyTo != "" {
				fmt.Printf("  reply_to: %s\n", res.Envelope.ReplyTo)
			}
			for _, k := range sortedKeys(res.Envelope.Meta) {
				fmt.Printf("  meta.%s: %s\n", k, res.Envelope.Meta[k])
			}
		}
	} else {
		fmt.Println(sprintBytes(info.Payload))
	}
}

// sortedKeys returns the keys of m in sorted order (stable CLI output).
func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
```

Add the required imports to `task.go` (alongside existing imports in the file):

```go
	"github.com/hibiken/asynq/tools/asynq/payload"
	"sort"
```

(If `sort` is already imported, do not add a duplicate. Group the `sort` stdlib import with the other stdlib imports at the top of the block.)

- [ ] **Step 6: 构建并测试**

Run: `cd tools && go build ./... && go vet ./asynq/cmd/...`
Expected: builds and vets cleanly.

- [ ] **Step 7: 烟测（需要可连的 Redis，可选）**

If a Redis is available, smoke-test against real data:

```bash
cd tools && go build -o /tmp/asynq ./asynq
/tmp/asynq task ls --queue=demo:default --state=pending            # 原样（现状）
/tmp/asynq task ls --queue=demo:default --state=pending --decode   # 单行解码
/tmp/asynq task inspect --queue=demo:default --id=<some-id> --decode
```

Expected: `--decode` output shows decoded JSON instead of `eyJ...`; `inspect --decode` prints a Payload section plus an Envelope section for nsp messages.

- [ ] **Step 8: 提交**

```bash
git add tools/asynq/cmd/task.go tools/asynq/cmd/root.go
git commit -m "feat(cli): add --decode to task ls/inspect, show payload+envelope"
```

---

## Self-Review

**1. Spec coverage** (against `docs/superpowers/specs/2026-06-30-payload-decode-and-dash-view-design.md`):
- §4 payload 包 (Decode/Format/Result/Envelope, 两层判断, 不依赖 nsp) → Task 1 ✓
- §4.4 测试（信封/纯base64/纯JSON/文本/二进制/nil/退化） → Task 1 测试表 ✓
- §5.1 State.payloadScrollOffset → Task 3 Step 1 ✓
- §5.2 弹窗解码 + 折行 + 滚动窗口 + envelope 摘要 + footer/help → Task 3 (渲染) + Task 4 (键位/footer/help) ✓
- §5.3 键位（弹窗滚动、选中/关闭重置）→ Task 4 ✓
- §5.4 渲染辅助测试 → Task 2（wrapLines/clampMaxOffset/modalDimsFromSize）✓
- §6.1 task list --decode → Task 6 ✓
- §6.2 task inspect --decode + 补全 payload → Task 6 ✓
- §7 变更清单全部覆盖 ✓
- §8 非目标：未 import nsp（payload 包仅用标准库）✓；未做深层递归 ✓；无 flag 时 CLI 现状不变（formatPayload 在 decode=false 时回落 sprintBytes）✓；未新增全屏查看器 ✓

**2. Placeholder scan:** 无 TBD/TODO；所有代码步骤含完整代码；命令含预期输出。✓

**3. Type consistency:**
- `payload.Decode(data []byte, indent bool) Result` / `payload.Format(data []byte, indent bool) string` — 在 Task 1 定义，Task 3/5/6 调用一致 ✓
- `payload.Result{Kind, Payload, Envelope}` 与 `payload.Envelope{Version,TraceID,SpanID,Sampled,HasSampled,ReplyTo,Meta}` — Task 1 定义，Task 3 `appendEnvelopeRows` / Task 6 `printTaskInfo` 字段引用一致 ✓
- `State.payloadScrollOffset` — Task 3 定义，Task 4 读写一致 ✓
- `buildModalRows(task, contentWidth)`、`modalMaxOffset(s, task)`、`modalDims/modalDimsFromSize`、`wrapLines`、`clampMaxOffset` — 定义与调用签名一致 ✓
- `formatPayload(p []byte, decode bool)` — Task 6 定义并在 list 函数中调用一致 ✓
- `printTaskInfo(info, decode)` 与 `taskInspect` 调用一致 ✓
