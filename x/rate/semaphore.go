package rate

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/hibiken/asynq"
	asynqcontext "github.com/hibiken/asynq/internal/context"
)

// NewSemaphore creates a new counting Semaphore.
func NewSemaphore(rco asynq.RedisConnOpt, name string, maxConcurrency int) *Semaphore {
	rc, ok := rco.MakeRedisClient().(redis.UniversalClient)
	if !ok {
		panic(fmt.Sprintf("rate.NewSemaphore: unsupported RedisConnOpt type %T", rco))
	}

	if maxConcurrency < 1 {
		panic("rate.NewSemaphore: maxConcurrency cannot be less than 1")
	}

	if len(strings.TrimSpace(name)) == 0 {
		panic("rate.NewSemaphore: name should not be empty")
	}

	return &Semaphore{
		rc:             rc,
		name:           name,
		maxConcurrency: maxConcurrency,
	}
}

// Semaphore is a distributed counting semaphore which can be used to set maxConcurrency across multiple asynq servers.
type Semaphore struct {
	rc             redis.UniversalClient
	maxConcurrency int
	name           string
}

// KEYS[1] -> asynq:sema:<scope>
// ARGV[1] -> max concurrency
// ARGV[2] -> current time in unix time
// ARGV[3] -> deadline in unix time
// ARGV[4] -> task ID
var acquireCmd = redis.NewScript(`
redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", tonumber(ARGV[2])-1)
local lockCount = redis.call("ZCARD", KEYS[1])

if (lockCount < tonumber(ARGV[1])) then
     redis.call("ZADD", KEYS[1], ARGV[3], ARGV[4])
     return true
else
    return false
end
`)

// Acquire will try to acquire lock on the counting semaphore.
// - Returns (true, nil), iff semaphore key exists and current value is less than maxConcurrency
// - Returns (false, nil) when lock cannot be acquired
// - Returns (false, error) otherwise
//
// The context.Context passed to Acquire must have a deadline set,
// this ensures that lock is released if the job goroutine crashes and does not call Release.
func (s *Semaphore) Acquire(ctx context.Context) (bool, error) {
	d, ok := ctx.Deadline()
	if !ok {
		return false, fmt.Errorf("provided context must have a deadline")
	}

	taskID, ok := asynqcontext.GetTaskID(ctx)
	if !ok {
		return false, fmt.Errorf("provided context is missing task ID value")
	}

	b, err := acquireCmd.Run(ctx, s.rc,
		[]string{semaphoreKey(s.name)},
		[]interface{}{
			s.maxConcurrency,
			time.Now().Unix(),
			d.Unix(),
			taskID,
		}...).Bool()
	if err == redis.Nil {
		return b, nil
	}

	return b, err
}

// Release will release the lock on the counting semaphore.
func (s *Semaphore) Release(ctx context.Context) error {
	taskID, ok := asynqcontext.GetTaskID(ctx)
	if !ok {
		return fmt.Errorf("provided context is missing task ID value")
	}

	n, err := s.rc.ZRem(ctx, semaphoreKey(s.name), taskID).Result()
	if err != nil {
		return fmt.Errorf("redis command failed: %v", err)
	}

	if n == 0 {
		return fmt.Errorf("no lock found for task %q", taskID)
	}

	return nil
}

// Close closes the connection with redis.
func (s *Semaphore) Close() error {
	return s.rc.Close()
}

func semaphoreKey(name string) string {
	return fmt.Sprintf("asynq:sema:%s", name)
}
