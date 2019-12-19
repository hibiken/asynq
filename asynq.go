package asynq

import "github.com/go-redis/redis/v7"

/*
TODOs:
- [P0] asynqmon kill <taskID>, asynqmon killall <qname>
- [P0] Better Payload API - Assigning int or any number type to Payload will be converted to float64 in handler
- [P0] Show elapsed time for InProgress tasks (asynqmon ls inprogress)
- [P0] Redis Memory Usage, Connection info in stats
- [P0] Processed, Failed count for today
- [P0] Go docs + CONTRIBUTION.md + Github issue template + License comment
- [P0] Redis Sentinel support
- [P1] Add Support for multiple queues and priority
- [P1] User defined max-retry count
*/

// Max retry count by default
const defaultMaxRetry = 25

// Task represents a task to be performed.
type Task struct {
	// Type indicates the kind of the task to be performed.
	Type string

	// Payload is an arbitrary data needed for task execution.
	// The value has to be serializable.
	Payload map[string]interface{}
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
