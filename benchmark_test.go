// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// Simple E2E Benchmark testing with no scheduled tasks and
// no retries.
func BenchmarkEndToEndSimple(b *testing.B) {
	const count = 100000
	for n := 0; n < b.N; n++ {
		b.StopTimer() // begin setup
		r := setup(b)
		client := NewClient(r)
		bg := NewBackground(r, &Config{
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
		handler := func(t *Task) error {
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
		r := setup(b)
		client := NewClient(r)
		bg := NewBackground(r, &Config{
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
		handler := func(t *Task) error {
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
