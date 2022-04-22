//go:build linux || bsd || darwin
// +build linux bsd darwin

package asynq

import (
	"os"
	"os/signal"

	"golang.org/x/sys/unix"
)

// waitForSignals waits for signals and handles them.
// It handles SIGTERM, SIGINT, and SIGTSTP.
// SIGTERM and SIGINT will signal the process to exit.
// SIGTSTP will signal the process to stop processing new tasks.
func (srv *Server) waitForSignals() {
	srv.logger.Info("Send signal TSTP to stop processing new tasks")
	srv.logger.Info("Send signal TERM or INT to terminate the process")

	signal.Notify(srv.sigs, os.Interrupt, unix.SIGTERM, unix.SIGINT, unix.SIGTSTP)
	for {
		sig := <-srv.sigs
		if sig == unix.SIGTSTP {
			srv.Stop()
			continue
		}
		break
	}
}

// SignalShutdown stops and shuts down the server.
func (srv *Server) SignalShutdown() {
	srv.sigs <- os.Interrupt
}

func (s *Scheduler) waitForSignals() {
	s.logger.Info("Send signal TERM or INT to stop the scheduler")
	signal.Notify(s.sigs, os.Interrupt, unix.SIGTERM, unix.SIGINT)
	<-s.sigs
}

// SignalShutdown stops and shuts down the scheduler.
func (s *Scheduler) SignalShutdown() {
	s.sigs <- os.Interrupt
}
