package middleware

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

const defaultTaskLockKeyPrefix = "asynq:lock:"

// TaskLock returns middleware that honors the asynq.Lock option configured on a task.
func TaskLock(r asynq.RedisConnOpt) asynq.MiddlewareFunc {
	rc, ok := r.MakeRedisClient().(redis.UniversalClient)
	if !ok {
		panic(fmt.Sprintf("asynq/middleware: unsupported RedisConnOpt type %T", r))
	}
	return TaskLockFromRedisClient(rc)
}

// TaskLockFromRedisClient returns middleware that honors the asynq.Lock option configured on a task.
func TaskLockFromRedisClient(c redis.UniversalClient) asynq.MiddlewareFunc {
	m := &taskLockMiddleware{client: c}
	return m.wrap
}

type taskLockMiddleware struct {
	client redis.UniversalClient
}

type taskLockConfig struct {
	key   string
	token string
	ttl   time.Duration
}

var releaseTaskLockCmd = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`)

func (m *taskLockMiddleware) wrap(next asynq.Handler) asynq.Handler {
	return asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error {
		cfg, ok, err := taskLockConfigFromTask(ctx, task)
		if err != nil || !ok {
			if err != nil {
				return err
			}
			return next.ProcessTask(ctx, task)
		}

		acquired, err := m.acquire(ctx, cfg.key, cfg.token, cfg.ttl)
		if err != nil {
			return err
		}
		if !acquired {
			return fmt.Errorf("task lock busy for key %q: %w", cfg.key, asynq.RevokeTask)
		}
		defer m.release(cfg.key, cfg.token)
		return next.ProcessTask(ctx, task)
	})
}

func (m *taskLockMiddleware) acquire(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	ok, err := m.client.SetNX(ctx, key, token, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("asynq/middleware: acquire task lock: %w", err)
	}
	return ok, nil
}

func (m *taskLockMiddleware) release(key, token string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = releaseTaskLockCmd.Run(ctx, m.client, []string{key}, token).Err()
}

func taskLockConfigFromTask(ctx context.Context, task *asynq.Task) (cfg taskLockConfig, ok bool, err error) {
	headers := task.Headers()
	if len(headers) == 0 || headers[asynq.TaskLockHeader] != "1" {
		return taskLockConfig{}, false, nil
	}
	taskID, ok := asynq.GetTaskID(ctx)
	if !ok {
		return taskLockConfig{}, false, fmt.Errorf("asynq/middleware: task lock requires task ID in context")
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return taskLockConfig{}, false, fmt.Errorf("asynq/middleware: task lock requires context deadline")
	}
	ttl := time.Until(deadline)
	if ttl <= 0 {
		return taskLockConfig{}, false, fmt.Errorf("asynq/middleware: task lock deadline already expired")
	}

	lockKey := headers[asynq.TaskLockKeyHeader]
	if strings.TrimSpace(lockKey) == "" {
		lockKey = "task:" + task.Type() + ":" + taskID
	}
	lockKey = defaultTaskLockKeyPrefix + lockKey
	return taskLockConfig{
		key:   lockKey,
		token: taskID,
		ttl:   ttl,
	}, true, nil
}
