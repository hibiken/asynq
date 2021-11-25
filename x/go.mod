module github.com/hibiken/asynq/x

go 1.16

require (
	github.com/go-redis/redis/v8 v8.11.4
	github.com/google/uuid v1.3.0
	github.com/hibiken/asynq v0.0.0-00010101000000-000000000000
	github.com/prometheus/client_golang v1.11.0
)

replace github.com/hibiken/asynq => ./..
