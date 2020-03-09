// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package log exports logging related types and functions.
package log

import (
	"io"
	stdlog "log"
)

func NewLogger(out io.Writer) *Logger {
	return &Logger{
		stdlog.New(out, "", stdlog.Ldate|stdlog.Ltime|stdlog.Lmicroseconds|stdlog.LUTC),
	}
}

type Logger struct {
	*stdlog.Logger
}

func (l *Logger) Info(format string, args ...interface{}) {
	format = "INFO: " + format
	l.Printf(format, args...)
}

func (l *Logger) Warn(format string, args ...interface{}) {
	format = "WARN: " + format
	l.Printf(format, args...)
}

func (l *Logger) Error(format string, args ...interface{}) {
	format = "ERROR: " + format
	l.Printf(format, args...)
}
