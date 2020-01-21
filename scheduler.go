// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"time"

	"github.com/hibiken/asynq/internal/rdb"
)

type scheduler struct {
	rdb *rdb.RDB

	// channel to communicate back to the long running "scheduler" goroutine.
	done chan struct{}

	// poll interval on average
	avgInterval time.Duration

	// list of queues to move the tasks into.
	qnames []string
}

func newScheduler(r *rdb.RDB, avgInterval time.Duration, qcfg map[string]uint) *scheduler {
	var qnames []string
	for q := range qcfg {
		qnames = append(qnames, q)
	}
	return &scheduler{
		rdb:         r,
		done:        make(chan struct{}),
		avgInterval: avgInterval,
		qnames:      qnames,
	}
}

func (s *scheduler) terminate() {
	logger.info("Scheduler shutting down...")
	// Signal the scheduler goroutine to stop polling.
	s.done <- struct{}{}
}

// start starts the "scheduler" goroutine.
func (s *scheduler) start() {
	go func() {
		for {
			select {
			case <-s.done:
				logger.info("Scheduler done")
				return
			case <-time.After(s.avgInterval):
				s.exec()
			}
		}
	}()
}

func (s *scheduler) exec() {
	if err := s.rdb.CheckAndEnqueue(s.qnames...); err != nil {
		logger.error("Could not enqueue scheduled tasks: %v", err)
	}
}
