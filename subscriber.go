// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
)

type subscriber struct {
	rdb *rdb.RDB

	// channel to communicate back to the long running "subscriber" goroutine.
	done chan struct{}

	// cancelations hold cancel functions for all in-progress tasks.
	cancelations *base.Cancelations
}

func newSubscriber(rdb *rdb.RDB, cancelations *base.Cancelations) *subscriber {
	return &subscriber{
		rdb:          rdb,
		done:         make(chan struct{}),
		cancelations: cancelations,
	}
}

func (s *subscriber) terminate() {
	logger.info("Subscriber shutting down...")
	// Signal the subscriber goroutine to stop.
	s.done <- struct{}{}
}

func (s *subscriber) start() {
	pubsub, err := s.rdb.CancelationPubSub()
	cancelCh := pubsub.Channel()
	if err != nil {
		logger.error("cannot subscribe to cancelation channel: %v", err)
		return
	}
	go func() {
		for {
			select {
			case <-s.done:
				pubsub.Close()
				logger.info("Subscriber done")
				return
			case msg := <-cancelCh:
				cancel := s.cancelations.Get(msg.Payload)
				if cancel != nil {
					cancel()
				}
			}
		}
	}()
}
