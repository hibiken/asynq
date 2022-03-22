module github.com/hibiken/asynq/tools

go 1.13

require (
	github.com/fatih/color v1.9.0
	github.com/go-redis/redis/v8 v8.11.4
	github.com/hibiken/asynq v0.21.0 /* TODO: Update this after release */
	github.com/hibiken/asynq/x v0.0.0-20220131170841-349f4c50fb1d
	github.com/mitchellh/go-homedir v1.1.0
	github.com/prometheus/client_golang v1.11.0
	github.com/spf13/afero v1.1.2 // indirect
	github.com/spf13/cobra v1.1.1
	github.com/spf13/viper v1.7.0
)

/* TODO: Remove this before release */
replace github.com/hibiken/asynq => ../
