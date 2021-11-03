// Package rate contains rate limiting strategies for asynq.Handler(s).
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

// NewSemaphore creates a counting Semaphore for the given scope with the given number of tokens.
func NewSemaphore(rco asynq.RedisConnOpt, scope string, maxTokens int) *Semaphore {
	rc, ok := rco.MakeRedisClient().(redis.UniversalClient)
	if !ok {
		panic(fmt.Sprintf("rate.NewSemaphore: unsupported RedisConnOpt type %T", rco))
	}

	if maxTokens < 1 {
		panic("rate.NewSemaphore: maxTokens cannot be less than 1")
	}

	if len(strings.TrimSpace(scope)) == 0 {
		panic("rate.NewSemaphore: scope should not be empty")
	}

	return &Semaphore{
		rc:        rc,
		scope:     scope,
		maxTokens: maxTokens,
	}
}

// Semaphore is a distributed counting semaphore which can be used to set maxTokens across multiple asynq servers.
type Semaphore struct {
	rc        redis.UniversalClient
	maxTokens int
	scope     string
}

// KEYS[1] -> asynq:sema:<scope>
// ARGV[1] -> max concurrency
// ARGV[2] -> current time in unix time
// ARGV[3] -> deadline in unix time
// ARGV[4] -> task ID
var acquireCmd = redis.NewScript(`
redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", tonumber(ARGV[2])-1)
local count = redis.call("ZCARD", KEYS[1])

if (count < tonumber(ARGV[1])) then
     redis.call("ZADD", KEYS[1], ARGV[3], ARGV[4])
     return 'true'
else
     return 'false'
end
`)

// Acquire attempts to acquire a token from the semaphore.
// - Returns (true, nil), iff semaphore key exists and current value is less than maxTokens
// - Returns (false, nil) when token cannot be acquired
// - Returns (false, error) otherwise
//
// The context.Context passed to Acquire must have a deadline set,
// this ensures that token is released if the job goroutine crashes and does not call Release.
func (s *Semaphore) Acquire(ctx context.Context) (bool, error) {
	d, ok := ctx.Deadline()
	if !ok {
		return false, fmt.Errorf("provided context must have a deadline")
	}

	taskID, ok := asynqcontext.GetTaskID(ctx)
	if !ok {
		return false, fmt.Errorf("provided context is missing task ID value")
	}

	return acquireCmd.Run(ctx, s.rc,
		[]string{semaphoreKey(s.scope)},
		s.maxTokens,
		time.Now().Unix(),
		d.Unix(),
		taskID,
	).Bool()
}

// Release will release the token on the counting semaphore.
func (s *Semaphore) Release(ctx context.Context) error {
	taskID, ok := asynqcontext.GetTaskID(ctx)
	if !ok {
		return fmt.Errorf("provided context is missing task ID value")
	}

	n, err := s.rc.ZRem(ctx, semaphoreKey(s.scope), taskID).Result()
	if err != nil {
		return fmt.Errorf("redis command failed: %v", err)
	}

	if n == 0 {
		return fmt.Errorf("no token found for task %q", taskID)
	}

	return nil
}

// Close closes the connection to redis.
func (s *Semaphore) Close() error {
	return s.rc.Close()
}

func semaphoreKey(scope string) string {
	return fmt.Sprintf("asynq:sema:%s", scope)
}
