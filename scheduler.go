// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/log"
)

type scheduler struct {
	logger *log.Logger
	broker base.Broker

	// channel to communicate back to the long running "scheduler" goroutine.
	done chan struct{}

	// poll interval on average
	avgInterval time.Duration

	// list of queues to move the tasks into.
	qnames []string
}

func newScheduler(l *log.Logger, b base.Broker, avgInterval time.Duration, qcfg map[string]int) *scheduler {
	var qnames []string
	for q := range qcfg {
		qnames = append(qnames, q)
	}
	return &scheduler{
		logger:      l,
		broker:      b,
		done:        make(chan struct{}),
		avgInterval: avgInterval,
		qnames:      qnames,
	}
}

func (s *scheduler) terminate() {
	s.logger.Info("Scheduler shutting down...")
	// Signal the scheduler goroutine to stop polling.
	s.done <- struct{}{}
}

// start starts the "scheduler" goroutine.
func (s *scheduler) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-s.done:
				s.logger.Info("Scheduler done")
				return
			case <-time.After(s.avgInterval):
				s.exec()
			}
		}
	}()
}

func (s *scheduler) exec() {
	if err := s.broker.CheckAndEnqueue(s.qnames...); err != nil {
		s.logger.Errorf("Could not enqueue scheduled tasks: %v", err)
	}
}
