// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package log exports logging related types and functions.
package log

import (
	"fmt"
	"io"
	stdlog "log"
	"os"
)

// Base supports logging with various log levels.
type Base interface {
	// Debug logs a message at Debug level.
	Debug(args ...interface{})

	// Info logs a message at Info level.
	Info(args ...interface{})

	// Warn logs a message at Warning level.
	Warn(args ...interface{})

	// Error logs a message at Error level.
	Error(args ...interface{})

	// Fatal logs a message at Fatal level
	// and process will exit with status set to 1.
	Fatal(args ...interface{})
}

// baseLogger is a wrapper object around log.Logger from the standard library.
// It supports logging at various log levels.
type baseLogger struct {
	*stdlog.Logger
}

// Debug logs a message at Debug level.
func (l *baseLogger) Debug(args ...interface{}) {
	l.prefixPrint("DEBUG: ", args...)
}

// Info logs a message at Info level.
func (l *baseLogger) Info(args ...interface{}) {
	l.prefixPrint("INFO: ", args...)
}

// Warn logs a message at Warning level.
func (l *baseLogger) Warn(args ...interface{}) {
	l.prefixPrint("WARN: ", args...)
}

// Error logs a message at Error level.
func (l *baseLogger) Error(args ...interface{}) {
	l.prefixPrint("ERROR: ", args...)
}

// Fatal logs a message at Fatal level
// and process will exit with status set to 1.
func (l *baseLogger) Fatal(args ...interface{}) {
	l.prefixPrint("FATAL: ", args...)
	os.Exit(1)
}

func (l *baseLogger) prefixPrint(prefix string, args ...interface{}) {
	args = append([]interface{}{prefix}, args...)
	l.Print(args...)
}

// newBase creates and returns a new instance of baseLogger.
func newBase(out io.Writer) *baseLogger {
	prefix := fmt.Sprintf("asynq: pid=%d ", os.Getpid())
	return &baseLogger{
		stdlog.New(out, prefix, stdlog.Ldate|stdlog.Ltime|stdlog.Lmicroseconds|stdlog.LUTC),
	}
}

// NewLogger creates and returns a new instance of Logger.
func NewLogger(base Base) *Logger {
	if base == nil {
		base = newBase(os.Stderr)
	}
	return &Logger{base}
}

// Logger logs message to io.Writer with various log levels.
type Logger struct {
	Base
}

func (l *Logger) Debugf(format string, args ...interface{}) {
	l.Debug(fmt.Sprintf(format, args...))
}

func (l *Logger) Infof(format string, args ...interface{}) {
	l.Info(fmt.Sprintf(format, args...))
}

func (l *Logger) Warnf(format string, args ...interface{}) {
	l.Warn(fmt.Sprintf(format, args...))
}

func (l *Logger) Errorf(format string, args ...interface{}) {
	l.Error(fmt.Sprintf(format, args...))
}

func (l *Logger) Fatalf(format string, args ...interface{}) {
	l.Fatal(fmt.Sprintf(format, args...))
}
