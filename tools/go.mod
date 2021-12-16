module github.com/hibiken/asynq/tools

go 1.13

require (
	github.com/fatih/color v1.9.0
	github.com/go-redis/redis/v8 v8.11.4
	github.com/google/uuid v1.3.0
	github.com/hibiken/asynq v0.19.0
	github.com/hibiken/asynq/x v0.0.0-00010101000000-000000000000
	github.com/mitchellh/go-homedir v1.1.0
	github.com/prometheus/client_golang v1.11.0
	github.com/spf13/cobra v1.1.1
	github.com/spf13/viper v1.7.0
)

replace (
	github.com/hibiken/asynq => ./..
	github.com/hibiken/asynq/x => ./../x
)
