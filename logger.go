package asynq

import (
	"io"
	"log"
	"os"
)

// global logger used in asynq package.
var logger = newLogger(os.Stderr)

func newLogger(out io.Writer) *asynqLogger {
	return &asynqLogger{
		log.New(out, "", log.Ldate|log.Ltime|log.Lmicroseconds|log.LUTC),
	}
}

type asynqLogger struct {
	*log.Logger
}

func (l *asynqLogger) info(format string, args ...interface{}) {
	format = "INFO: " + format
	l.Printf(format, args...)
}

func (l *asynqLogger) warn(format string, args ...interface{}) {
	format = "WARN: " + format
	l.Printf(format, args...)
}

func (l *asynqLogger) error(format string, args ...interface{}) {
	format = "ERROR: " + format
	l.Printf(format, args...)
}
