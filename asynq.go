package asynq

import "github.com/go-redis/redis/v7"

/*
TODOs:
- [P0] command to retry tasks from "retry", "dead" queue
- [P0] Go docs + CONTRIBUTION.md + Github issue template
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
type RedisConfig struct {
	Addr     string
	Password string

	// DB specifies which redis database to select.
	DB int
}

func newRedisClient(config *RedisConfig) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     config.Addr,
		Password: config.Password,
		DB:       config.DB,
	})
}
