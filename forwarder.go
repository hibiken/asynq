// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/log"
)

// A forwarder is responsible for moving scheduled and retry tasks to pending state
// so that the tasks get processed by the workers.
type forwarder struct {
	logger *log.Logger
	broker base.Broker

	// channel to communicate back to the long running "forwarder" goroutine.
	done chan struct{}

	// list of queue names to check and enqueue.
	queues []string

	// poll interval on average
	avgInterval time.Duration
}

type forwarderParams struct {
	logger   *log.Logger
	broker   base.Broker
	queues   []string
	interval time.Duration
}

func newForwarder(params forwarderParams) *forwarder {
	return &forwarder{
		logger:      params.logger,
		broker:      params.broker,
		done:        make(chan struct{}),
		queues:      params.queues,
		avgInterval: params.interval,
	}
}

func (f *forwarder) shutdown() {
	f.logger.DebugContext(context.Background(), "Forwarder shutting down", "component", "forwarder")
	// Signal the forwarder goroutine to stop polling.
	f.done <- struct{}{}
}

// start starts the "forwarder" goroutine.
func (f *forwarder) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		timer := time.NewTimer(f.avgInterval)
		for {
			select {
			case <-f.done:
				f.logger.DebugContext(context.Background(), "Forwarder done", "component", "forwarder")
				return
			case <-timer.C:
				f.exec()
				timer.Reset(f.avgInterval)
			}
		}
	}()
}

func (f *forwarder) exec() {
	if err := f.broker.ForwardIfReady(f.queues...); err != nil {
		f.logger.ErrorContext(context.Background(), "Failed to forward scheduled tasks", "component", "forwarder", "error", err)
	}
}
