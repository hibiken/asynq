// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"testing"

	"github.com/go-redis/redis/v7"
	h "github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
)

func BenchmarkDone(b *testing.B) {
	r := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   8,
	})
	h.FlushDB(b, r)

	// populate in-progress queue with messages
	var inProgress []*base.TaskMessage
	for i := 0; i < 40; i++ {
		inProgress = append(inProgress,
			h.NewTaskMessage("send_email", map[string]interface{}{"subject": "hello", "recipient_id": 123}))
	}
	h.SeedInProgressQueue(b, r, inProgress)

	rdb := NewRDB(r)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		msg := h.NewTaskMessage("reindex", map[string]interface{}{"config": "path/to/config/file"})
		r.LPush(base.InProgressQueue, h.MustMarshal(b, msg))
		b.StartTimer()

		rdb.Done(msg)
	}
}
