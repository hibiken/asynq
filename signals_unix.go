// +build linux bsd darwin

package asynq

import (
	"os"
	"os/signal"

	"golang.org/x/sys/unix"

	"github.com/hibiken/asynq/internal/base"
)

// waitForSignals waits for signals and handles them.
// It handles SIGTERM, SIGINT, and SIGTSTP.
// SIGTERM and SIGINT will signal the process to exit.
// SIGTSTP will signal the process to stop processing new tasks.
func (bg *Background) waitForSignals() {
	bg.logger.Info("Send signal TSTP to stop processing new tasks")
	bg.logger.Info("Send signal TERM or INT to terminate the process")

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, unix.SIGTERM, unix.SIGINT, unix.SIGTSTP)
	for {
		sig := <-sigs
		if sig == unix.SIGTSTP {
			bg.processor.stop()
			bg.ps.SetStatus(base.StatusStopped)
			continue
		}
		break
	}
}
