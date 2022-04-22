//go:build windows
// +build windows

package asynq

import (
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
	srv.logger.Info("Send signal TERM or INT to terminate the process")
	signal.Notify(srv.sigs, os.Interrupt, windows.SIGTERM, windows.SIGINT)
	<-srv.sigs
}

// SignalShutdown stops and shuts down the server.
func (srv *Server) SignalShutdown() {
	srv.sigs <- os.Interrupt
}

func (s *Scheduler) waitForSignals() {
	s.logger.Info("Send signal TERM or INT to stop the scheduler")
	signal.Notify(s.sigs, os.Interrupt, windows.SIGTERM, windows.SIGINT)
	<-s.sigs
}

// SignalShutdown stops and shuts down the scheduler.
func (s *Scheduler) SignalShutdown() {
	s.sigs <- os.Interrupt
}
