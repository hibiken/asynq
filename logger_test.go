package asynq

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
)

func TestWithStructuredLogger(t *testing.T) {
	var buf bytes.Buffer
	slogger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cfg := Config{LogLevel: DebugLevel}
	WithStructuredLogger(slogger)(&cfg)

	if cfg.StructuredLogger != slogger {
		t.Fatal("StructuredLogger not set on Config")
	}

	logger := newLogger(cfg.Logger, cfg.StructuredLogger, cfg.LogLevel)
	logger.InfoContext(context.Background(), "test", "component", "server")

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if rec["msg"] != "test" {
		t.Errorf("msg = %v, want test", rec["msg"])
	}
}

func TestNewLoggerLegacyPath(t *testing.T) {
	logger := newLogger(nil, nil, InfoLevel)
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
	logger.Info("legacy path ok")
}

func TestNewLoggerLegacyFatalLevel(t *testing.T) {
	logger := newLogger(nil, nil, FatalLevel)
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
}

func TestWithStructuredLoggerForScheduler(t *testing.T) {
	slogger := slog.Default()
	opts := &SchedulerOpts{}
	WithStructuredLoggerForScheduler(slogger)(opts)
	if opts.StructuredLogger != slogger {
		t.Fatal("StructuredLogger not set on SchedulerOpts")
	}
}

func TestNewLoggerStructuredWithAllLogLevels(t *testing.T) {
	tests := []struct {
		name     string
		logLevel LogLevel
		wantMsg  string
	}{
		{name: "WarnLevel", logLevel: WarnLevel, wantMsg: "warn level"},
		{name: "ErrorLevel", logLevel: ErrorLevel, wantMsg: "error level"},
		{name: "DebugLevel", logLevel: DebugLevel, wantMsg: "debug level"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			slogger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
			logger := newLogger(nil, slogger, tc.logLevel)

			switch tc.logLevel {
			case WarnLevel:
				logger.WarnContext(context.Background(), tc.wantMsg)
			case ErrorLevel:
				logger.ErrorContext(context.Background(), tc.wantMsg)
			case DebugLevel:
				logger.DebugContext(context.Background(), tc.wantMsg)
			}

			var rec map[string]any
			if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
				t.Fatalf("invalid json: %v; got %q", err, buf.String())
			}
			if rec["msg"] != tc.wantMsg {
				t.Errorf("msg = %v, want %q", rec["msg"], tc.wantMsg)
			}
		})
	}
}

func TestNewLoggerStructuredDefaultsUnspecifiedLevel(t *testing.T) {
	var buf bytes.Buffer
	slogger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	logger := newLogger(nil, slogger, LogLevel(0)) // level_unspecified
	logger.DebugContext(context.Background(), "hidden at default info level")
	if buf.Len() != 0 {
		t.Errorf("DebugContext should be suppressed when level is unspecified (defaults to Info), got %q", buf.String())
	}

	logger.InfoContext(context.Background(), "unspecified defaults to info")

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if rec["msg"] != "unspecified defaults to info" {
		t.Errorf("msg = %v, want unspecified defaults to info", rec["msg"])
	}
}
