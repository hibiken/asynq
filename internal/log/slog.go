package log

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	asynqcontext "github.com/hibiken/asynq/internal/context"
)

type slogLogger struct {
	logger *slog.Logger
	parent *Logger
}

// NewStructuredLogger creates a Logger that writes using log/slog.
func NewStructuredLogger(l *slog.Logger, level Level) *Logger {
	if l == nil {
		l = slog.Default()
	}
	parent := &Logger{level: level}
	parent.slog = &slogLogger{logger: l, parent: parent}
	return parent
}

func (l *Logger) DebugContext(ctx context.Context, msg string, args ...any) {
	l.logContext(ctx, DebugLevel, msg, args...)
}

func (l *Logger) InfoContext(ctx context.Context, msg string, args ...any) {
	l.logContext(ctx, InfoLevel, msg, args...)
}

func (l *Logger) WarnContext(ctx context.Context, msg string, args ...any) {
	l.logContext(ctx, WarnLevel, msg, args...)
}

func (l *Logger) ErrorContext(ctx context.Context, msg string, args ...any) {
	l.logContext(ctx, ErrorLevel, msg, args...)
}

func (l *Logger) logContext(ctx context.Context, level Level, msg string, args ...any) {
	if l.slog != nil {
		l.slog.log(ctx, level, msg, args)
		return
	}
	if !l.canLogAt(level) {
		return
	}
	formatted := formatLegacyMessage(msg, args...)
	switch level {
	case DebugLevel:
		l.base.Debug(formatted)
	case InfoLevel:
		l.base.Info(formatted)
	case WarnLevel:
		l.base.Warn(formatted)
	case ErrorLevel, FatalLevel:
		l.base.Error(formatted)
	}
}

func (s *slogLogger) log(ctx context.Context, level Level, msg string, args []any) {
	if !s.parent.canLogAt(level) {
		return
	}
	attrs := taskAttrs(ctx)
	if len(args) > 0 {
		attrs = append(attrs, args...)
	}
	switch level {
	case DebugLevel:
		s.logger.DebugContext(ctx, msg, attrs...)
	case InfoLevel:
		s.logger.InfoContext(ctx, msg, attrs...)
	case WarnLevel:
		s.logger.WarnContext(ctx, msg, attrs...)
	case ErrorLevel:
		s.logger.ErrorContext(ctx, msg, attrs...)
	case FatalLevel:
		s.logger.ErrorContext(ctx, msg, attrs...)
		os.Exit(1)
	}
}

func taskAttrs(ctx context.Context) []any {
	var attrs []any
	if id, ok := asynqcontext.GetTaskID(ctx); ok {
		attrs = append(attrs, "task_id", id)
	}
	if qname, ok := asynqcontext.GetQueueName(ctx); ok {
		attrs = append(attrs, "queue", qname)
	}
	if n, ok := asynqcontext.GetRetryCount(ctx); ok {
		attrs = append(attrs, "retry_count", n)
	}
	if n, ok := asynqcontext.GetMaxRetry(ctx); ok {
		attrs = append(attrs, "max_retry", n)
	}
	return attrs
}

func formatLegacyMessage(msg string, args ...any) string {
	if len(args) == 0 {
		return msg
	}
	// Treat args as alternating key-value pairs when possible for legacy path.
	if len(args)%2 == 0 {
		var b string
		b = msg
		for i := 0; i < len(args); i += 2 {
			b += " " + sprintPair(args[i], args[i+1])
		}
		return b
	}
	return msg + " " + sprintArgs(args...)
}

func sprintPair(k, v any) string {
	return sprintArgs(k) + "=" + sprintArgs(v)
}

func sprintArgs(v ...any) string {
	switch len(v) {
	case 0:
		return ""
	case 1:
		return fmtSprint(v[0])
	default:
		var s strings.Builder
		for i, a := range v {
			if i > 0 {
				s.WriteString(" ")
			}
			s.WriteString(fmtSprint(a))
		}
		return s.String()
	}
}

func fmtSprint(v any) string {
	return fmt.Sprint(v)
}
