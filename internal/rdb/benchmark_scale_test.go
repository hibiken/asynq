// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/testutil"
	"github.com/redis/go-redis/v9"
)

func redisZ(score int64, member string) redis.Z {
	return redis.Z{Score: float64(score), Member: member}
}

// seedActive fills the active list and lease zset of the default queue with n
// tasks and returns their messages (index 0 is the oldest, i.e. closest to the
// list tail, matching how Dequeue pushes new tasks to the head).
func seedActive(b *testing.B, r *RDB, n int, leaseAt time.Time) []*base.TaskMessage {
	b.Helper()
	ctx := context.Background()
	msgs := make([]*base.TaskMessage, n)
	pipe := r.client.Pipeline()
	for i := 0; i < n; i++ {
		m := testutil.NewTaskMessageWithQueue(fmt.Sprintf("t%d", i), nil, base.DefaultQueueName)
		msgs[i] = m
		encoded, err := base.EncodeMessage(m)
		if err != nil {
			b.Fatal(err)
		}
		pipe.HSet(ctx, base.TaskKey(m.Queue, m.ID), "msg", encoded, "state", "active")
		pipe.LPush(ctx, base.ActiveKey(m.Queue), m.ID)
		pipe.ZAdd(ctx, base.LeaseKey(m.Queue), redisZ(leaseAt.Unix(), m.ID))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		b.Fatal(err)
	}
	return msgs
}

// BenchmarkDoneActiveListSize measures Done when the active list holds many
// tasks (think: many workers across the cluster). The finished task is taken
// from the head (most recently started), the middle, or the tail (oldest).
func BenchmarkDoneActiveListSize(b *testing.B) {
	for _, n := range []int{100, 10000, 100000} {
		for _, pos := range []string{"head", "middle", "tail"} {
			b.Run(fmt.Sprintf("n=%d/%s", n, pos), func(b *testing.B) {
				r := setup(b)
				testutil.FlushDB(b, r.client)
				msgs := seedActive(b, r, n, time.Now().Add(time.Minute))
				idx := map[string]int{"head": n - 1, "middle": n / 2, "tail": 0}[pos]
				target := msgs[idx]
				ctx := context.Background()
				encoded, _ := base.EncodeMessage(target)
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					if err := r.Done(ctx, target); err != nil {
						b.Fatalf("Done failed: %v", err)
					}
					// Restore the task so the next iteration finds it again at
					// the same relative position.
					b.StopTimer()
					pipe := r.client.Pipeline()
					pipe.HSet(ctx, base.TaskKey(target.Queue, target.ID), "msg", encoded, "state", "active")
					switch pos {
					case "head":
						pipe.LPush(ctx, base.ActiveKey(target.Queue), target.ID)
					case "tail":
						pipe.RPush(ctx, base.ActiveKey(target.Queue), target.ID)
					default:
						// insert next to the middle element
						mid := msgs[n/2+1]
						pipe.LInsertBefore(ctx, base.ActiveKey(target.Queue), mid.ID, target.ID)
					}
					pipe.ZAdd(ctx, base.LeaseKey(target.Queue), redisZ(time.Now().Add(time.Minute).Unix(), target.ID))
					if _, err := pipe.Exec(ctx); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
				}
			})
		}
	}
}

// BenchmarkLRemCount isolates the Redis cost of LREM with count 0 (full scan)
// against count 1 (stop at first match) on a large list.
func BenchmarkLRemCount(b *testing.B) {
	for _, n := range []int{10000, 100000} {
		for _, count := range []int64{0, 1} {
			b.Run(fmt.Sprintf("n=%d/count=%d", n, count), func(b *testing.B) {
				r := setup(b)
				testutil.FlushDB(b, r.client)
				ctx := context.Background()
				key := base.ActiveKey(base.DefaultQueueName)
				pipe := r.client.Pipeline()
				for i := 0; i < n; i++ {
					pipe.LPush(ctx, key, fmt.Sprintf("id-%d", i))
				}
				pipe.Exec(ctx)
				target := fmt.Sprintf("id-%d", n-1) // head: best case for count=1
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := r.client.LRem(ctx, key, count, target).Err(); err != nil {
						b.Fatal(err)
					}
					b.StopTimer()
					r.client.LPush(ctx, key, target)
					b.StartTimer()
				}
			})
		}
	}
}

// BenchmarkDequeueEmptyQueues measures an idle poll over N empty queues.
func BenchmarkDequeueEmptyQueues(b *testing.B) {
	for _, nq := range []int{1, 3, 10} {
		b.Run(fmt.Sprintf("queues=%d", nq), func(b *testing.B) {
			r := setup(b)
			testutil.FlushDB(b, r.client)
			qnames := make([]string, nq)
			for i := range qnames {
				qnames[i] = fmt.Sprintf("q%d", i)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := r.Dequeue(qnames...); err == nil {
					b.Fatal("expected no processable task")
				}
			}
		})
	}
}

// BenchmarkListLeaseExpired measures the recoverer's scan when many leases
// expired at once (e.g. after several servers crashed).
func BenchmarkListLeaseExpired(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("expired=%d", n), func(b *testing.B) {
			r := setup(b)
			testutil.FlushDB(b, r.client)
			seedActive(b, r, n, time.Now().Add(-time.Hour))
			cutoff := time.Now().Add(-30 * time.Second)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				msgs, err := r.ListLeaseExpired(cutoff, base.DefaultQueueName)
				if err != nil {
					b.Fatal(err)
				}
				if len(msgs) == 0 {
					b.Fatal("no expired messages returned")
				}
			}
		})
	}
}

// BenchmarkStateEventCost compares an Enqueue (script + state PUBLISH, which is
// what the library does today) with the script round trip alone.
func BenchmarkStateEventCost(b *testing.B) {
	ctx := context.Background()
	b.Run("Enqueue(with state publish)", func(b *testing.B) {
		r := setup(b)
		testutil.FlushDB(b, r.client)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m := testutil.NewTaskMessage("t", nil)
			if err := r.Enqueue(ctx, m); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Enqueue script only", func(b *testing.B) {
		r := setup(b)
		testutil.FlushDB(b, r.client)
		r.client.SAdd(ctx, base.AllQueues, base.DefaultQueueName)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m := testutil.NewTaskMessage("t", nil)
			encoded, _ := base.EncodeMessage(m)
			keys := []string{base.TaskKey(m.Queue, m.ID), base.PendingKey(m.Queue)}
			if err := enqueueCmd.Run(ctx, r.client, keys, encoded, m.ID, time.Now().UnixNano()).Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("stateEvent encode only", func(b *testing.B) {
		m := testutil.NewTaskMessage("t", nil)
		for i := 0; i < b.N; i++ {
			_ = stateEvent(m, base.TaskStatePending)
		}
	})
}
