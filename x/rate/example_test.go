package rate_test

import (
	"context"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	"github.com/hibiken/asynq/x/rate"
)

type RateLimitError struct {
	RetryIn time.Duration
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("rate limited (retry in  %v)", e.RetryIn)
}

func ExampleNewSemaphore() {
	redisConnOpt := asynq.RedisClientOpt{Addr: ":6379"}
	sema := rate.NewSemaphore(redisConnOpt, "my_queue", 10)
	// call sema.Close() when appropriate

	_ = asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error {
		ok, err := sema.Acquire(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return &RateLimitError{RetryIn: 30 * time.Second}
		}

		// Make sure to release the token once we're done.
		defer sema.Release(ctx)

		// Process task
		return nil
	})
}
