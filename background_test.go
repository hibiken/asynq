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
	bg := NewBackground(r, 10)

	// no-op handler
	h := func(task *Task) error {
		return nil
	}

	bg.start(HandlerFunc(h))

	client.Process(&Task{
		Type:    "send_email",
		Payload: map[string]interface{}{"recipient_id": 123},
	}, time.Now())

	client.Process(&Task{
		Type:    "send_email",
		Payload: map[string]interface{}{"recipient_id": 456},
	}, time.Now().Add(time.Hour))

	bg.stop()
}
