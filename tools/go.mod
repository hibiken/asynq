module github.com/hibiken/asynq/tools

go 1.13

require (
	github.com/coreos/go-etcd v2.0.0+incompatible // indirect
	github.com/cpuguy83/go-md2man v1.0.10 // indirect
	github.com/fatih/color v1.9.0
	github.com/go-redis/redis/v7 v7.4.0
	github.com/google/uuid v1.1.1
	github.com/hibiken/asynq v0.4.0
	github.com/mitchellh/go-homedir v1.1.0
	github.com/spf13/cast v1.3.1
	github.com/spf13/cobra v1.0.0
	github.com/spf13/viper v1.6.2
	github.com/ugorji/go/codec v0.0.0-20181204163529-d75b2dcb6bc8 // indirect
)

replace github.com/hibiken/asynq => ./..
