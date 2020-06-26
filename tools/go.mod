module github.com/hibiken/asynq/tools

go 1.13

require (
	github.com/go-redis/redis/v7 v7.2.0
	github.com/hibiken/asynq v0.4.0
	github.com/mitchellh/go-homedir v1.1.0
	github.com/rs/xid v1.2.1
	github.com/spf13/cast v1.3.1
	github.com/spf13/cobra v0.0.5
	github.com/spf13/viper v1.6.2
)

replace github.com/hibiken/asynq => ./..
