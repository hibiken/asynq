// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package log exports logging related types and functions.
package log

import (
	"io"
	stdlog "log"
	"os"
)

// NewLogger creates and returns a new instance of Logger.
func NewLogger(out io.Writer) *Logger {
	return &Logger{
		stdlog.New(out, "", stdlog.Ldate|stdlog.Ltime|stdlog.Lmicroseconds|stdlog.LUTC),
	}
}

// Logger is a wrapper object around log.Logger from the standard library.
// It supports logging at various log levels.
type Logger struct {
	*stdlog.Logger
}

// Debug logs a message at Debug level.
func (l *Logger) Debug(format string, args ...interface{}) {
	format = "DEBUG: " + format
	l.Printf(format, args...)
}

// Info logs a message at Info level.
func (l *Logger) Info(format string, args ...interface{}) {
	format = "INFO: " + format
	l.Printf(format, args...)
}

// Warn logs a message at Warning level.
func (l *Logger) Warn(format string, args ...interface{}) {
	format = "WARN: " + format
	l.Printf(format, args...)
}

// Error logs a message at Error level.
func (l *Logger) Error(format string, args ...interface{}) {
	format = "ERROR: " + format
	l.Printf(format, args...)
}

// Fatal logs a message at Fatal level
// and process will exit with status set to 1.
func (l *Logger) Fatal(format string, args ...interface{}) {
	format = "FATAL: " + format
	l.Printf(format, args...)
	os.Exit(1)
}
