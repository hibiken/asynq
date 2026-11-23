//go:build linux || dragonfly || freebsd || netbsd || openbsd || darwin

package asynq

import (
	"context"
	"os"
	"os/signal"

	"golang.org/x/sys/unix"
)

// waitForSignals waits for signals and handles them.
// It handles SIGTERM, SIGINT, and SIGTSTP.
// SIGTERM and SIGINT will signal the process to exit.
// SIGTSTP will signal the process to stop processing new tasks.
func (srv *Server) waitForSignals() {
	srv.logger.InfoContext(context.Background(), "Send signal TSTP to stop processing new tasks", "component", "server")
	srv.logger.InfoContext(context.Background(), "Send signal TERM or INT to terminate the process", "component", "server")

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, unix.SIGTERM, unix.SIGINT, unix.SIGTSTP)
	for {
		sig := <-sigs
		if sig == unix.SIGTSTP {
			srv.Stop()
			continue
		} else {
			srv.Stop()
			break
		}
	}
}

func (s *Scheduler) waitForSignals() {
	s.logger.InfoContext(context.Background(), "Send signal TERM or INT to stop the scheduler", "component", "scheduler")
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, unix.SIGTERM, unix.SIGINT)
	<-sigs
}
