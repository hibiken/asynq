package asynq

import (
	"log"
	"time"

	"github.com/hibiken/asynq/internal/rdb"
)

type scheduler struct {
	rdb *rdb.RDB

	// channel to communicate back to the long running "scheduler" goroutine.
	done chan struct{}

	// poll interval on average
	avgInterval time.Duration
}

func newScheduler(r *rdb.RDB, avgInterval time.Duration) *scheduler {
	return &scheduler{
		rdb:         r,
		done:        make(chan struct{}),
		avgInterval: avgInterval,
	}
}

func (s *scheduler) terminate() {
	log.Println("[INFO] Scheduler shutting down...")
	// Signal the scheduler goroutine to stop polling.
	s.done <- struct{}{}
}

// start starts the "scheduler" goroutine.
func (s *scheduler) start() {
	go func() {
		for {
			select {
			case <-s.done:
				log.Println("[INFO] Scheduler done.")
				return
			case <-time.After(s.avgInterval):
				s.exec()
			}
		}
	}()
}

func (s *scheduler) exec() {
	if err := s.rdb.CheckAndEnqueue(); err != nil {
		log.Printf("[ERROR] could not forward scheduled tasks: %v\n", err)
	}
}
