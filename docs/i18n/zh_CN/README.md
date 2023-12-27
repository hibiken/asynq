<img src="https://user-images.githubusercontent.com/11155743/114697792-ffbfa580-9d26-11eb-8e5b-33bef69476dc.png" alt="Asynq logo" width="360px" />

# 一个用GO编写的 简单, 可靠 , 高性能 分布式任务队列

[![GoDoc](https://godoc.org/github.com/hibiken/asynq?status.svg)](https://godoc.org/github.com/hibiken/asynq)
[![Go Report Card](https://goreportcard.com/badge/github.com/hibiken/asynq)](https://goreportcard.com/report/github.com/hibiken/asynq)
![Build Status](https://github.com/hibiken/asynq/workflows/build/badge.svg)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Gitter chat](https://badges.gitter.im/go-asynq/gitter.svg)](https://gitter.im/go-asynq/community)

Asynq 是一个 Go 编写的库，提供任务队列能力，使Worker可以异步处理任务。Asynq 由 [Redis](https://redis.io/) 驱动，旨在方便扩展，容易上手。

Asynq 的工作原理简述:

- 客户端推送任务到队列
- 服务端从队列拉取任务，并为每个任务启动一个 goroutine 去处理它
- 任务可以被并行处理

任务队列常被用来跨多机器调度任务。队列可以有多个Worker, Server和Broker，以实现高可用和水平扩展。

**典型用例**

![Task Queue Diagram](https://user-images.githubusercontent.com/11155743/116358505-656f5f80-a806-11eb-9c16-94e49dab0f99.jpg)

## 特性

- 可靠消费[保证任务至少被消费一次](https://www.cloudcomputingpatterns.org/at_least_once_delivery/)
- 支持调度任务
- 支持失败任务[重试](https://github.com/hibiken/asynq/wiki/Task-Retry)
- Worker崩溃时的任务自动恢复
- 支持[加权优先级队列](https://github.com/hibiken/asynq/wiki/Queue-Priority#weighted-priority)
- 支持[严格优先级队列](https://github.com/hibiken/asynq/wiki/Queue-Priority#strict-priority)
- 低延迟添加任务
- 支持[unique选项]（https://github.com/hibiken/asynq/wiki/Unique-Tasks）保证任务不重复
- 支持[单个任务设置超时和截止时间](https://github.com/hibiken/asynq/wiki/Task-Timeout-and-Cancelation)
- 支持[任务聚合编组](https://github.com/hibiken/asynq/wiki/Task-aggregation) 用于一次批量处理多个连续操作
- 支持[中间件](https://github.com/hibiken/asynq/wiki/Handler-Deep-Dive)，我们有灵活的handler接口
- 支持[队列暂停](/tools/asynq/README.md#pause) 即暂停分发任务
- 支持[定时任务](https://github.com/hibiken/asynq/wiki/Periodic-Tasks)
- 支持[Redis Cluster](https://github.com/hibiken/asynq/wiki/Redis-Cluster) 实现自动分片和高可用
- 支持[Redis Sentinel](https://github.com/hibiken/asynq/wiki/Automatic-Failover) 实现高可用性
- 与[Prometheus](https://prometheus.io/) 集成，以收集和可视化队列运行情况
- 支持[Web UI](#web-ui) 检查和控制队列/任务
- 支持[CLI](#command-line-tool) 检查和控制队列/任务


## 稳定性和兼容性

**状态**：该库目前正在处于**深度开发中**，API可能随时发生大的变化

> ☝️ **重要提示**：当前主要版本为 v0 版本（`v0.x.x`），以适应快速开发和快速迭代，同时获得用户的早期反馈（感谢您的使用反馈！_）。在 `v1.0.0` 发布之前，公共 API 可能会在没有主要版本更新的情况下发生变化。

## 快速开始

确保([Go1.14+](https://golang.org/dl/))已安装.

通过创建一个文件夹然后在该文件夹中运行`go mod init github.com/your/repo`（[了解更多](https://blog.golang.org/using-go-modules)）来初始化您的项目。然后使用 [`go get`](https://golang.org/cmd/go/#hdr-Add_dependencies_to_current_module_and_install_them) 命令安装 Asynq 库：

```sh
go get -u github.com/hibiken/asynq
```

确保Redis服务器4.0+在本地或[Docker](https://hub.docker.com/_/redis)

然后写一个封装任务创建和任务处理的包（定义任务和处理方式）。

```go
package tasks

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "time"
    "github.com/hibiken/asynq"
)

// A list of task types.
const (
    TypeEmailDelivery   = "email:deliver"
    TypeImageResize     = "image:resize"
)

type EmailDeliveryPayload struct {
    UserID     int
    TemplateID string
}

type ImageResizePayload struct {
    SourceURL string
}

//----------------------------------------------
// Write a function NewXXXTask to create a task.
// A task consists of a type and a payload.
//----------------------------------------------

func NewEmailDeliveryTask(userID int, tmplID string) (*asynq.Task, error) {
    payload, err := json.Marshal(EmailDeliveryPayload{UserID: userID, TemplateID: tmplID})
    if err != nil {
        return nil, err
    }
    return asynq.NewTask(TypeEmailDelivery, payload), nil
}

func NewImageResizeTask(src string) (*asynq.Task, error) {
    payload, err := json.Marshal(ImageResizePayload{SourceURL: src})
    if err != nil {
        return nil, err
    }
    // task options can be passed to NewTask, which can be overridden at enqueue time.
    return asynq.NewTask(TypeImageResize, payload, asynq.MaxRetry(5), asynq.Timeout(20 * time.Minute)), nil
}

//---------------------------------------------------------------
// Write a function HandleXXXTask to handle the input task.
// Note that it satisfies the asynq.HandlerFunc interface.
//
// Handler doesn't need to be a function. You can define a type
// that satisfies asynq.Handler interface. See examples below.
//---------------------------------------------------------------

func HandleEmailDeliveryTask(ctx context.Context, t *asynq.Task) error {
    var p EmailDeliveryPayload
    if err := json.Unmarshal(t.Payload(), &p); err != nil {
        return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
    }
    log.Printf("Sending Email to User: user_id=%d, template_id=%s", p.UserID, p.TemplateID)
    // Email delivery code ...
    return nil
}

// ImageProcessor implements asynq.Handler interface.
type ImageProcessor struct {
    // ... fields for struct
}

func (processor *ImageProcessor) ProcessTask(ctx context.Context, t *asynq.Task) error {
    var p ImageResizePayload
    if err := json.Unmarshal(t.Payload(), &p); err != nil {
        return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
    }
    log.Printf("Resizing image: src=%s", p.SourceURL)
    // Image resizing code ...
    return nil
}

func NewImageProcessor() *ImageProcessor {
	return &ImageProcessor{}
}
```

在您的项目中，导入上述包并使用 [`Client`](https://pkg.go.dev/github.com/hibiken/asynq?tab=doc#Client) 将任务推送至队列中。

```go
package main

import (
    "log"
    "time"

    "github.com/hibiken/asynq"
    "your/app/package/tasks"
)

const redisAddr = "127.0.0.1:6379"

func main() {
    client := asynq.NewClient(asynq.RedisClientOpt{Addr: redisAddr})
    defer client.Close()

    // ------------------------------------------------------
    // Example 1: Enqueue task to be processed immediately.
    //            Use (*Client).Enqueue method.
    // ------------------------------------------------------

    task, err := tasks.NewEmailDeliveryTask(42, "some:template:id")
    if err != nil {
        log.Fatalf("could not create task: %v", err)
    }
    info, err := client.Enqueue(task)
    if err != nil {
        log.Fatalf("could not enqueue task: %v", err)
    }
    log.Printf("enqueued task: id=%s queue=%s", info.ID, info.Queue)


    // ------------------------------------------------------------
    // Example 2: Schedule task to be processed in the future.
    //            Use ProcessIn or ProcessAt option.
    // ------------------------------------------------------------

    info, err = client.Enqueue(task, asynq.ProcessIn(24*time.Hour))
    if err != nil {
        log.Fatalf("could not schedule task: %v", err)
    }
    log.Printf("enqueued task: id=%s queue=%s", info.ID, info.Queue)


    // ----------------------------------------------------------------------------
    // Example 3: Set other options to tune task processing behavior.
    //            Options include MaxRetry, Queue, Timeout, Deadline, Unique etc.
    // ----------------------------------------------------------------------------

    task, err = tasks.NewImageResizeTask("https://example.com/myassets/image.jpg")
    if err != nil {
        log.Fatalf("could not create task: %v", err)
    }
    info, err = client.Enqueue(task, asynq.MaxRetry(10), asynq.Timeout(3 * time.Minute))
    if err != nil {
        log.Fatalf("could not enqueue task: %v", err)
    }
    log.Printf("enqueued task: id=%s queue=%s", info.ID, info.Queue)
}
```

然后启动一个worker消费这些任务。您可以使用 [`Server`](https://pkg.go.dev/github.com/hibiken/asynq?tab=doc#Server) 提供的 [`Handler`](https:// pkg.go.dev/github.com/hibiken/asynq?tab=doc#Handler) 来处理任务。

推荐使用 [`ServeMux`](https://pkg.go.dev/github.com/hibiken/asynq?tab=doc#ServeMux) 来创建handler，当然你也可以用标准库的 [`net/http` ](https://golang.org/pkg/net/http/) 。

```go
package main

import (
    "log"

    "github.com/hibiken/asynq"
    "your/app/package/tasks"
)

const redisAddr = "127.0.0.1:6379"

func main() {
    srv := asynq.NewServer(
        asynq.RedisClientOpt{Addr: redisAddr},
        asynq.Config{
            // Specify how many concurrent workers to use
            Concurrency: 10,
            // Optionally specify multiple queues with different priority.
            Queues: map[string]int{
                "critical": 6,
                "default":  3,
                "low":      1,
            },
            // See the godoc for other configuration options
        },
    )

    // mux maps a type to a handler
    mux := asynq.NewServeMux()
    mux.HandleFunc(tasks.TypeEmailDelivery, tasks.HandleEmailDeliveryTask)
    mux.Handle(tasks.TypeImageResize, tasks.NewImageProcessor())
    // ...register other handlers...

    if err := srv.Run(mux); err != nil {
        log.Fatalf("could not run server: %v", err)
    }
}
```

有关该库的更详细的说明，可以参阅 [Getting Started](https://github.com/hibiken/asynq/wiki/Getting-Started).

要了解有关 `asynq` 特性和 API 的更多信息，请参阅包注释文档 [godoc](https://godoc.org/github.com/hibiken/asynq)。

## Web UI

[Asynqmon](https://github.com/hibiken/asynqmon) 是一个基于 Web ，可以用来监控和管理 Asynq 队列/任务的工具.

以下是 Asynqmon 的一些截图：

**队列**

![Web UI Queues View](https://user-images.githubusercontent.com/11155743/114697016-07327f00-9d26-11eb-808c-0ac841dc888e.png)

**任务**

![Web UI TasksView](https://user-images.githubusercontent.com/11155743/114697070-1f0a0300-9d26-11eb-855c-d3ec263865b7.png)

**指标**
<img width="1532" alt="Screen Shot 2021-12-19 at 4 37 19 PM" src="https://user-images.githubusercontent.com/10953044/146777420-cae6c476-bac6-469c-acce-b2f6584e8707.png">

**支持自适应的夜间模式**

![Web UI设置支持自适应的夜晚模式](https://user-images.githubusercontent.com/11155743/114697149-3517c380-9d26-11eb-9f7a-ae2dd00aad5b.png)
该工具的更多信息，请参阅该 [README](https://github.com/hibiken/asynqmon#readme)。

## CLI工具

Asynq 提供了一个命令行工具来检查队列/任务的运行状态

安装Asynq CLI工具:

```sh
go install github.com/hibiken/asynq/tools/asynq
```

下面是 `asynq dash` 命令的示例：

![Gif](/docs/assets/dash.gif)

更多信息请参阅 [README](/tools/asynq/README.md)

## 一起参与进来

我们欢迎并感谢来自社区的（GitHub 问题/PR、 [Gitter 频道](https://gitter.im/go-asynq/community) 反馈）

贡献前请参阅[贡献指南](/CONTRIBUTING.md)

## 许可

Copyright (c) 2019-present [Ken Hibino](https://github.com/hibiken) and [Contributors](https://github.com/hibiken/asynq/graphs/contributors). `Asynq` is free and open-source software licensed under the [MIT License](https://github.com/hibiken/asynq/blob/master/LICENSE). Official logo was created by [Vic Shóstak](https://github.com/koddr) and distributed under [Creative Commons](https://creativecommons.org/publicdomain/zero/1.0/) license (CC0 1.0 Universal).
