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
			t := Task{Type: fmt.Sprintf("task%d", i), Payload: Payload{"data": i}}
			client.Process(&t, time.Now())
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
			t := Task{Type: fmt.Sprintf("task%d", i), Payload: Payload{"data": i}}
			client.Process(&t, time.Now())
		}
		for i := 0; i < count; i++ {
			t := Task{Type: fmt.Sprintf("scheduled%d", i), Payload: Payload{"data": i}}
			client.Process(&t, time.Now().Add(time.Second))
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
