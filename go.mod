module github.com/hibiken/asynq

go 1.14

require (
	gitee.com/liujinsuo/tool v0.0.0-20221015162352-c86de30b0ade // indirect
	github.com/go-redis/redis/v8 v8.11.5
	github.com/golang/protobuf v1.5.2
	github.com/google/go-cmp v0.5.9
	github.com/google/uuid v1.2.0
	github.com/robfig/cron/v3 v3.0.1
	github.com/spf13/cast v1.5.0
	go.uber.org/goleak v1.1.12
	golang.org/x/sys v0.0.0-20220722155257-8c9f86f7a55f
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	google.golang.org/genproto v0.0.0-20200526211855-cb27e3aa2013 // indirect
	google.golang.org/protobuf v1.26.0
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
)

replace github.com/hibiken/asynq => github.com/jinsuojinsuo/asynq v0.0.0-20221010120852-a757de9634b2
