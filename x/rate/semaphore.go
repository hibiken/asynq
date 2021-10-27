package rate

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-redis/redis/v8"
	"github.com/hibiken/asynq"
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

var acquireCmd = redis.NewScript(`
local lockCount = tonumber(redis.call('GET', KEYS[1]))

if (not lockCount or lockCount < tonumber(ARGV[1])) then
  redis.call('INCR', KEYS[1])
  return true
else
  return false
end
`)

// Acquire will try to acquire lock on the counting semaphore.
// - Returns true, iff semaphore key exists and current value is less than maxConcurrency
// - Returns false otherwise
func (s *Semaphore) Acquire(ctx context.Context) bool {
	val, _ := acquireCmd.Run(ctx, s.rc, []string{semaphoreKey(s.name)}, []interface{}{s.maxConcurrency}...).Bool()
	return val
}

// Release will release the lock on the counting semaphore.
func (s *Semaphore) Release(ctx context.Context) {
	s.rc.Decr(ctx, semaphoreKey(s.name))
}

// Close closes the connection with redis.
func (s *Semaphore) Close() error {
	return s.rc.Close()
}

func semaphoreKey(name string) string {
	return fmt.Sprintf("asynq:sema:{%s}", name)
}
