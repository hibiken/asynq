// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"errors"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/log"
)

type subscriber struct {
	logger *log.Logger
	broker base.Broker

	// channel to communicate back to the long running "subscriber" goroutine.
	done chan struct{}

	// cancelations hold cancel functions for all active tasks.
	cancelations *base.Cancelations

	// time to wait before retrying to connect to redis.
	retryTimeout time.Duration
}

type subscriberParams struct {
	logger       *log.Logger
	broker       base.Broker
	cancelations *base.Cancelations
}

func newSubscriber(params subscriberParams) *subscriber {
	return &subscriber{
		logger:       params.logger,
		broker:       params.broker,
		done:         make(chan struct{}),
		cancelations: params.cancelations,
		retryTimeout: 5 * time.Second,
	}
}

func (s *subscriber) shutdown() {
	s.logger.Debug("Subscriber shutting down...")
	// Signal the subscriber goroutine to stop.
	s.done <- struct{}{}
}

func (s *subscriber) pubSubUtilSuccessfully() (pubsub *redis.PubSub, err error) {
	for {
		pubsub, err = s.broker.CancelationPubSub()
		if err != nil {
			s.logger.Errorf("cannot subscribe to cancelation channel: %v", err)
			select {
			case <-time.After(s.retryTimeout):
				continue
			case <-s.done:
				return nil, errors.New("subscriber done")
			}
		}
		return
	}
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
		pubsub, err = s.pubSubUtilSuccessfully()
		if err != nil {
			s.logger.Debug(err.Error())
			return
		}

		for {
			// (*PubSub).Channel() 可重入，返回的channel不会重新初始化
			cancelCh := pubsub.Channel()

			select {
			case <-s.done:
				pubsub.Close()
				s.logger.Debug("Subscriber done")
				return
			case msg, ok := <-cancelCh:
				if !ok {
					pubsub, err = s.pubSubUtilSuccessfully()
					if err != nil {
						s.logger.Debug(err.Error())
						return
					}
					continue
				}
				cancel, ok := s.cancelations.Get(msg.Payload)
				if ok {
					cancel()
				}
			}
		}
	}()
}
