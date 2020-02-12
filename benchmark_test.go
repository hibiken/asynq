// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// Simple E2E Benchmark testing with no scheduled tasks and retries.
func BenchmarkEndToEndSimple(b *testing.B) {
	const count = 100000
	for n := 0; n < b.N; n++ {
		b.StopTimer() // begin setup
		setup(b)
		redis := &RedisClientOpt{
			Addr: redisAddr,
			DB:   redisDB,
		}
		client := NewClient(redis)
		bg := NewBackground(redis, &Config{
			Concurrency: 10,
			RetryDelayFunc: func(n int, err error, t *Task) time.Duration {
				return time.Second
			},
		})
		// Create a bunch of tasks
		for i := 0; i < count; i++ {
			t := NewTask(fmt.Sprintf("task%d", i), map[string]interface{}{"data": i})
			client.Schedule(t, time.Now())
		}

		var wg sync.WaitGroup
		wg.Add(count)
		handler := func(ctx context.Context, t *Task) error {
			wg.Done()
			return nil
		}
		b.StartTimer() // end setup

		bg.start(HandlerFunc(handler))
		wg.Wait()

		b.StopTimer() // begin teardown
		bg.stop()
		b.StartTimer() // end teardown
	}
}

// E2E benchmark with scheduled tasks and retries.
func BenchmarkEndToEnd(b *testing.B) {
	const count = 100000
	for n := 0; n < b.N; n++ {
		b.StopTimer() // begin setup
		rand.Seed(time.Now().UnixNano())
		setup(b)
		redis := &RedisClientOpt{
			Addr: redisAddr,
			DB:   redisDB,
		}
		client := NewClient(redis)
		bg := NewBackground(redis, &Config{
			Concurrency: 10,
			RetryDelayFunc: func(n int, err error, t *Task) time.Duration {
				return time.Second
			},
		})
		// Create a bunch of tasks
		for i := 0; i < count; i++ {
			t := NewTask(fmt.Sprintf("task%d", i), map[string]interface{}{"data": i})
			client.Schedule(t, time.Now())
		}
		for i := 0; i < count; i++ {
			t := NewTask(fmt.Sprintf("scheduled%d", i), map[string]interface{}{"data": i})
			client.Schedule(t, time.Now().Add(time.Second))
		}

		var wg sync.WaitGroup
		wg.Add(count * 2)
		handler := func(ctx context.Context, t *Task) error {
			// randomly fail 1% of tasks
			if rand.Intn(100) == 1 {
				return fmt.Errorf(":(")
			}
			wg.Done()
			return nil
		}
		b.StartTimer() // end setup

		bg.start(HandlerFunc(handler))
		wg.Wait()

		b.StopTimer() // begin teardown
		bg.stop()
		b.StartTimer() // end teardown
	}
}

// Simple E2E Benchmark testing with no scheduled tasks and retries with multiple queues.
func BenchmarkEndToEndMultipleQueues(b *testing.B) {
	// number of tasks to create for each queue
	const (
		highCount    = 20000
		defaultCount = 20000
		lowCount     = 20000
	)
	for n := 0; n < b.N; n++ {
		b.StopTimer() // begin setup
		setup(b)
		redis := &RedisClientOpt{
			Addr: redisAddr,
			DB:   redisDB,
		}
		client := NewClient(redis)
		bg := NewBackground(redis, &Config{
			Concurrency: 10,
			Queues: map[string]uint{
				"high":    6,
				"default": 3,
				"low":     1,
			},
		})
		// Create a bunch of tasks
		for i := 0; i < highCount; i++ {
			t := NewTask(fmt.Sprintf("task%d", i), map[string]interface{}{"data": i})
			client.Schedule(t, time.Now(), Queue("high"))
		}
		for i := 0; i < defaultCount; i++ {
			t := NewTask(fmt.Sprintf("task%d", i), map[string]interface{}{"data": i})
			client.Schedule(t, time.Now())
		}
		for i := 0; i < lowCount; i++ {
			t := NewTask(fmt.Sprintf("task%d", i), map[string]interface{}{"data": i})
			client.Schedule(t, time.Now(), Queue("low"))
		}

		var wg sync.WaitGroup
		wg.Add(highCount + defaultCount + lowCount)
		handler := func(ctx context.Context, t *Task) error {
			wg.Done()
			return nil
		}
		b.StartTimer() // end setup

		bg.start(HandlerFunc(handler))
		wg.Wait()

		b.StopTimer() // begin teardown
		bg.stop()
		b.StartTimer() // end teardown
	}
}
