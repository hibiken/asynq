module github.com/hibiken/asynq/tools

go 1.13

require (
	github.com/armon/consul-api v0.0.0-20180202201655-eb2c6b5be1b6 // indirect
	github.com/coreos/go-etcd v2.0.0+incompatible // indirect
	github.com/cpuguy83/go-md2man v1.0.10 // indirect
	github.com/fatih/color v1.9.0
	github.com/go-redis/redis/v7 v7.4.0
	github.com/golang/protobuf v1.4.1 // indirect
	github.com/google/uuid v1.2.0
	github.com/hibiken/asynq v0.17.1
	github.com/mitchellh/go-homedir v1.1.0
	github.com/spf13/cast v1.3.1
	github.com/spf13/cobra v1.1.1
	github.com/spf13/viper v1.7.0
	github.com/ugorji/go v1.1.4 // indirect
	github.com/ugorji/go/codec v0.0.0-20181204163529-d75b2dcb6bc8 // indirect
	github.com/xordataexchange/crypt v0.0.3-0.20170626215501-b2862e3d0a77 // indirect
)

replace github.com/hibiken/asynq => ./..
