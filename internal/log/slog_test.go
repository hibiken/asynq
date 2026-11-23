package log

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/base"
	asynqcontext "github.com/hibiken/asynq/internal/context"
)

func TestStructuredLoggerDebugContext(t *testing.T) {
	var buf bytes.Buffer
	h := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := NewStructuredLogger(slog.New(h), DebugLevel)

	logger.DebugContext(context.Background(), "hello", "component", "test")

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("invalid json: %v; got %q", err, buf.String())
	}
	if rec["msg"] != "hello" {
		t.Errorf("msg = %v, want hello", rec["msg"])
	}
	if rec["component"] != "test" {
		t.Errorf("component = %v, want test", rec["component"])
	}
}

func TestStructuredLoggerRespectsLevel(t *testing.T) {
	var buf bytes.Buffer
	h := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := NewStructuredLogger(slog.New(h), InfoLevel)

	logger.DebugContext(context.Background(), "hidden")
	if buf.Len() != 0 {
		t.Errorf("DebugContext should be suppressed at InfoLevel, got %q", buf.String())
	}
}

func TestStructuredLoggerTaskAttrs(t *testing.T) {
	var buf bytes.Buffer
	h := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := NewStructuredLogger(slog.New(h), DebugLevel)

	msg := &base.TaskMessage{
		ID:    "task-123",
		Queue: "default",
	}
	ctx, cancel := asynqcontext.New(context.Background(), msg, time.Now().Add(time.Minute))
	defer cancel()

	logger.InfoContext(ctx, "processing")

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("invalid json: %v; got %q", err, buf.String())
	}
	if rec["task_id"] != "task-123" {
		t.Errorf("task_id = %v, want task-123", rec["task_id"])
	}
	if rec["queue"] != "default" {
		t.Errorf("queue = %v, want default", rec["queue"])
	}
}

func TestLegacyLoggerContextFallback(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(newBase(&buf))
	logger.SetLevel(DebugLevel)

	logger.InfoContext(context.Background(), "hello", "key", "value")

	got := buf.String()
	if !strings.Contains(got, "INFO: hello key=value") {
		t.Errorf("got %q, want message with key=value", got)
	}
}

func TestStructuredLoggerWarnAndErrorContext(t *testing.T) {
	tests := []struct {
		name string
		log  func(*Logger)
		want string
	}{
		{
			name: "WarnContext",
			log: func(l *Logger) {
				l.WarnContext(context.Background(), "warn msg", "component", "test")
			},
			want: "warn msg",
		},
		{
			name: "ErrorContext",
			log: func(l *Logger) {
				l.ErrorContext(context.Background(), "error msg", "component", "test")
			},
			want: "error msg",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			h := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
			logger := NewStructuredLogger(slog.New(h), DebugLevel)

			tc.log(logger)

			rec := parseJSONLog(t, buf.Bytes())
			if rec["msg"] != tc.want {
				t.Errorf("msg = %v, want %q", rec["msg"], tc.want)
			}
			if rec["component"] != "test" {
				t.Errorf("component = %v, want test", rec["component"])
			}
		})
	}
}

func TestStructuredLoggerNonContextMethods(t *testing.T) {
	tests := []struct {
		name string
		log  func(*Logger)
		want string
	}{
		{
			name: "Debug",
			log:  func(l *Logger) { l.Debug("debug msg") },
			want: "debug msg",
		},
		{
			name: "Info",
			log:  func(l *Logger) { l.Info("info msg") },
			want: "info msg",
		},
		{
			name: "Warn",
			log:  func(l *Logger) { l.Warn("warn msg") },
			want: "warn msg",
		},
		{
			name: "Error",
			log:  func(l *Logger) { l.Error("error msg") },
			want: "error msg",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			h := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
			logger := NewStructuredLogger(slog.New(h), DebugLevel)

			tc.log(logger)

			rec := parseJSONLog(t, buf.Bytes())
			if rec["msg"] != tc.want {
				t.Errorf("msg = %v, want %q", rec["msg"], tc.want)
			}
		})
	}
}

func TestNewStructuredLoggerNilUsesDefault(t *testing.T) {
	logger := NewStructuredLogger(nil, InfoLevel)
	if logger == nil || logger.slog == nil {
		t.Fatal("expected structured logger with default slog backend")
	}
}

func TestStructuredLoggerTaskAttrsRetryFields(t *testing.T) {
	var buf bytes.Buffer
	h := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := NewStructuredLogger(slog.New(h), DebugLevel)

	msg := &base.TaskMessage{
		ID:      "task-456",
		Queue:   "critical",
		Retry:   5,
		Retried: 2,
	}
	ctx, cancel := asynqcontext.New(context.Background(), msg, time.Now().Add(time.Minute))
	defer cancel()

	logger.InfoContext(ctx, "retrying")

	rec := parseJSONLog(t, buf.Bytes())
	if rec["task_id"] != "task-456" {
		t.Errorf("task_id = %v, want task-456", rec["task_id"])
	}
	if rec["queue"] != "critical" {
		t.Errorf("queue = %v, want critical", rec["queue"])
	}
	if rec["retry_count"] != float64(2) {
		t.Errorf("retry_count = %v, want 2", rec["retry_count"])
	}
	if rec["max_retry"] != float64(5) {
		t.Errorf("max_retry = %v, want 5", rec["max_retry"])
	}
}

func TestLegacyLoggerContextLevels(t *testing.T) {
	tests := []struct {
		name       string
		log        func(*Logger)
		wantSubstr string
	}{
		{
			name: "WarnContext",
			log: func(l *Logger) {
				l.WarnContext(context.Background(), "warn", "k", "v")
			},
			wantSubstr: "WARN: warn k=v",
		},
		{
			name: "ErrorContext",
			log: func(l *Logger) {
				l.ErrorContext(context.Background(), "err", "k", "v")
			},
			wantSubstr: "ERROR: err k=v",
		},
		{
			name: "DebugContext",
			log: func(l *Logger) {
				l.DebugContext(context.Background(), "dbg")
			},
			wantSubstr: "DEBUG: dbg",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := NewLogger(newBase(&buf))
			logger.SetLevel(DebugLevel)

			tc.log(logger)

			if !strings.Contains(buf.String(), tc.wantSubstr) {
				t.Errorf("got %q, want substring %q", buf.String(), tc.wantSubstr)
			}
		})
	}
}

func TestLegacyLoggerContextMessageFormatting(t *testing.T) {
	tests := []struct {
		name       string
		msg        string
		args       []any
		wantSubstr string
	}{
		{
			name:       "no extra args",
			msg:        "plain",
			wantSubstr: "INFO: plain",
		},
		{
			name:       "odd number of args",
			msg:        "partial",
			args:       []any{"only", "one", "pair"},
			wantSubstr: "INFO: partial only one pair",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := NewLogger(newBase(&buf))
			logger.SetLevel(InfoLevel)

			logger.InfoContext(context.Background(), tc.msg, tc.args...)

			if !strings.Contains(buf.String(), tc.wantSubstr) {
				t.Errorf("got %q, want substring %q", buf.String(), tc.wantSubstr)
			}
		})
	}
}

func TestLegacyLoggerContextRespectsLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(newBase(&buf))
	logger.SetLevel(WarnLevel)

	logger.InfoContext(context.Background(), "hidden")
	if buf.Len() != 0 {
		t.Errorf("InfoContext should be suppressed at WarnLevel, got %q", buf.String())
	}
}

func parseJSONLog(t *testing.T, data []byte) map[string]any {
	t.Helper()
	var rec map[string]any
	if err := json.Unmarshal(data, &rec); err != nil {
		t.Fatalf("invalid json: %v; got %q", err, string(data))
	}
	return rec
}
