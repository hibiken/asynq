//go:build windows

package asynq

import (
	"context"
	"os"
	"os/signal"

	"golang.org/x/sys/windows"
)

// waitForSignals waits for signals and handles them.
// It handles SIGTERM and SIGINT.
// SIGTERM and SIGINT will signal the process to exit.
//
// Note: Currently SIGTSTP is not supported for windows build.
func (srv *Server) waitForSignals() {
	srv.logger.InfoContext(context.Background(), "Send signal TERM or INT to terminate the process", "component", "server")
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, windows.SIGTERM, windows.SIGINT)
	<-sigs
}

func (s *Scheduler) waitForSignals() {
	s.logger.InfoContext(context.Background(), "Send signal TERM or INT to stop the scheduler", "component", "scheduler")
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, windows.SIGTERM, windows.SIGINT)
	<-sigs
}
