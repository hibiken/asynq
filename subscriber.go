// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"sync"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
)

type subscriber struct {
	logger Logger
	rdb    *rdb.RDB

	// channel to communicate back to the long running "subscriber" goroutine.
	done chan struct{}

	// cancelations hold cancel functions for all in-progress tasks.
	cancelations *base.Cancelations
}

func newSubscriber(l Logger, rdb *rdb.RDB, cancelations *base.Cancelations) *subscriber {
	return &subscriber{
		logger:       l,
		rdb:          rdb,
		done:         make(chan struct{}),
		cancelations: cancelations,
	}
}

func (s *subscriber) terminate() {
	s.logger.Info("Subscriber shutting down...")
	// Signal the subscriber goroutine to stop.
	s.done <- struct{}{}
}

func (s *subscriber) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		var (
			pubsub *redis.PubSub
			err    error
		)
		// Try until successfully connect to Redis.
		for {
			pubsub, err = s.rdb.CancelationPubSub()
			if err != nil {
				s.logger.Error("cannot subscribe to cancelation channel: %v", err)
				select {
				case <-time.After(5 * time.Second): // retry in 5s
					continue
				case <-s.done:
					s.logger.Info("Subscriber done")
					return
				}
			}
			break
		}
		cancelCh := pubsub.Channel()
		for {
			select {
			case <-s.done:
				pubsub.Close()
				s.logger.Info("Subscriber done")
				return
			case msg := <-cancelCh:
				cancel, ok := s.cancelations.Get(msg.Payload)
				if ok {
					cancel()
				}
			}
		}
	}()
}
