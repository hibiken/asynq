//go:build windows

package asynq

import (
	"os"
	"os/signal"
	"syscall"
)

// waitForSignals waits for signals and handles them.
// It handles SIGTERM and SIGINT.
// SIGTERM and SIGINT will signal the process to exit.
//
// Note: Currently SIGTSTP is not supported for windows build.
// Note: Currently Ctrl-C on Windows signals syscall.SIGINT, not windows.SIGINT
func (srv *Server) waitForSignals() {
	srv.logger.Info("Send signal TERM or INT to terminate the process")
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	<-sigs
}

func (s *Scheduler) waitForSignals() {
	s.logger.Info("Send signal TERM or INT to stop the scheduler")
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	<-sigs
}
