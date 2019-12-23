package asynq

import "github.com/go-redis/redis/v7"

/*
TODOs:
- [P0] asynqmon kill <taskID>, asynqmon killall <qname>
- [P0] Pagination for `asynqmon ls` command
- [P0] Show elapsed time for InProgress tasks (asynqmon ls inprogress)
- [P0] Processed, Failed count for today
- [P0] Go docs + CONTRIBUTION.md + Github issue template + License comment
- [P0] Redis Sentinel support
- [P1] Add Support for multiple queues and priority
*/

// Task represents a task to be performed.
type Task struct {
	// Type indicates the kind of the task to be performed.
	Type string

	// Payload holds data needed for the task execution.
	Payload Payload
}

// RedisConfig specifies redis configurations.
// TODO(hibiken): Support more configuration.
type RedisConfig struct {
	Addr     string
	Password string

	// DB specifies which redis database to select.
	DB int
}

func newRedisClient(cfg *RedisConfig) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
}
