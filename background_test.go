// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"go.uber.org/goleak"
)

func TestBackground(t *testing.T) {
	// https://github.com/go-redis/redis/issues/1029
	ignoreOpt := goleak.IgnoreTopFunction("github.com/go-redis/redis/v7/internal/pool.(*ConnPool).reaper")
	defer goleak.VerifyNoLeaks(t, ignoreOpt)

	r := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15,
	})
	client := NewClient(r)
	bg := NewBackground(r, &Config{
		Concurrency: 10,
	})

	// no-op handler
	h := func(task *Task) error {
		return nil
	}

	bg.start(HandlerFunc(h))

	client.Schedule(NewTask("send_email", map[string]interface{}{"recipient_id": 123}), time.Now())

	client.Schedule(NewTask("send_email", map[string]interface{}{"recipient_id": 456}), time.Now().Add(time.Hour))

	bg.stop()
}
