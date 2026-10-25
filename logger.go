package asynq

import (
	"log/slog"

	"github.com/hibiken/asynq/internal/log"
)

// WithStructuredLogger returns a function that sets cfg.StructuredLogger.
//
// When StructuredLogger is set, asynq uses log/slog context-aware logging
// for internal logs. Config.Logger is ignored in that case.
//
// Example:
//
//	cfg := asynq.Config{Concurrency: 10, LogLevel: asynq.InfoLevel}
//	asynq.WithStructuredLogger(slog.Default())(&cfg)
//	srv := asynq.NewServer(redisOpt, cfg)
func WithStructuredLogger(l *slog.Logger) func(*Config) {
	return func(c *Config) {
		c.StructuredLogger = l
	}
}

// WithStructuredLoggerForScheduler returns a function that sets opts.StructuredLogger.
//
// When StructuredLogger is set, asynq uses log/slog context-aware logging
// for internal logs. SchedulerOpts.Logger is ignored in that case.
func WithStructuredLoggerForScheduler(l *slog.Logger) func(*SchedulerOpts) {
	return func(o *SchedulerOpts) {
		o.StructuredLogger = l
	}
}

func newLogger(logger Logger, structuredLogger *slog.Logger, logLevel LogLevel) *log.Logger {
	loglevel := logLevel
	if loglevel == level_unspecified {
		loglevel = InfoLevel
	}
	level := toInternalLogLevel(loglevel)
	if structuredLogger != nil {
		return log.NewStructuredLogger(structuredLogger, level)
	}
	l := log.NewLogger(logger)
	l.SetLevel(level)
	return l
}
