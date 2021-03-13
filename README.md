# Asynq

![Build Status](https://github.com/hibiken/asynq/workflows/build/badge.svg)
[![GoDoc](https://godoc.org/github.com/hibiken/asynq?status.svg)](https://godoc.org/github.com/hibiken/asynq)
[![Go Report Card](https://goreportcard.com/badge/github.com/hibiken/asynq)](https://goreportcard.com/report/github.com/hibiken/asynq)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Gitter chat](https://badges.gitter.im/go-asynq/gitter.svg)](https://gitter.im/go-asynq/community)

## Overview

Asynq is a Go library for queueing tasks and processing them asynchronously with workers. It's backed by Redis and is designed to be scalable yet easy to get started.

Highlevel overview of how Asynq works:

- Client puts task on a queue
- Server pulls task off queues and starts a worker goroutine for each task
- Tasks are processed concurrently by multiple workers

Task queues are used as a mechanism to distribute work across multiple machines.  
A system can consist of multiple worker servers and brokers, giving way to high availability and horizontal scaling.

![Task Queue Diagram](/docs/assets/overview.png)

## Stability and Compatibility

**Important Note**: Current major version is zero (v0.x.x) to accomodate rapid development and fast iteration while getting early feedback from users (Feedback on APIs are appreciated!). The public API could change without a major version update before v1.0.0 release.

**Status**: The library is currently undergoing heavy development with frequent, breaking API changes.

## Features

- Guaranteed [at least one execution](https://www.cloudcomputingpatterns.org/at_least_once_delivery/) of a task
- Scheduling of tasks
- Durability since tasks are written to Redis
- [Retries](https://github.com/hibiken/asynq/wiki/Task-Retry) of failed tasks
- Automatic recovery of tasks in the event of a worker crash
- [Weighted priority queues](https://github.com/hibiken/asynq/wiki/Priority-Queues#weighted-priority-queues)
- [Strict priority queues](https://github.com/hibiken/asynq/wiki/Priority-Queues#strict-priority-queues)
- Low latency to add a task since writes are fast in Redis
- De-duplication of tasks using [unique option](https://github.com/hibiken/asynq/wiki/Unique-Tasks)
- Allow [timeout and deadline per task](https://github.com/hibiken/asynq/wiki/Task-Timeout-and-Cancelation)
- [Flexible handler interface with support for middlewares](https://github.com/hibiken/asynq/wiki/Handler-Deep-Dive)
- [Ability to pause queue](/tools/asynq/README.md#pause) to stop processing tasks from the queue
- [Periodic Tasks](https://github.com/hibiken/asynq/wiki/Periodic-Tasks)
- [Support Redis Cluster](https://github.com/hibiken/asynq/wiki/Redis-Cluster) for automatic sharding and high availability
- [Support Redis Sentinels](https://github.com/hibiken/asynq/wiki/Automatic-Failover) for high availability
- [Web UI](#web-ui) to inspect and remote-control queues and tasks
- [CLI](#command-line-tool) to inspect and remote-control queues and tasks

## Quickstart

First, make sure you are running a Redis server locally.

```sh
$ redis-server
```

Next, write a package that encapsulates task creation and task handling.

```go
package tasks

import (
    "fmt"

    "github.com/hibiken/asynq"
)

// A list of task types.
const (
    TypeEmailDelivery   = "email:deliver"
    TypeImageResize     = "image:resize"
)

//----------------------------------------------
// Write a function NewXXXTask to create a task.
// A task consists of a type and a payload.
//----------------------------------------------

func NewEmailDeliveryTask(userID int, tmplID string) *asynq.Task {
    payload := map[string]interface{}{"user_id": userID, "template_id": tmplID}
    return asynq.NewTask(TypeEmailDelivery, payload)
}

func NewImageResizeTask(src string) *asynq.Task {
    payload := map[string]interface{}{"src": src}
    return asynq.NewTask(TypeImageResize, payload)
}

//---------------------------------------------------------------
// Write a function HandleXXXTask to handle the input task.
// Note that it satisfies the asynq.HandlerFunc interface.
//
// Handler doesn't need to be a function. You can define a type
// that satisfies asynq.Handler interface. See examples below.
//---------------------------------------------------------------

func HandleEmailDeliveryTask(ctx context.Context, t *asynq.Task) error {
    userID, err := t.Payload.GetInt("user_id")
    if err != nil {
        return err
    }
    tmplID, err := t.Payload.GetString("template_id")
    if err != nil {
        return err
    }
    fmt.Printf("Send Email to User: user_id = %d, template_id = %s\n", userID, tmplID)
    // Email delivery code ...
    return nil
}

// ImageProcessor implements asynq.Handler interface.
type ImageProcessor struct {
    // ... fields for struct
}

func (p *ImageProcessor) ProcessTask(ctx context.Context, t *asynq.Task) error {
    src, err := t.Payload.GetString("src")
    if err != nil {
        return err
    }
    fmt.Printf("Resize image: src = %s\n", src)
    // Image resizing code ...
    return nil
}

func NewImageProcessor() *ImageProcessor {
    // ... return an instance
}
```

In your application code, import the above package and use [`Client`](https://pkg.go.dev/github.com/hibiken/asynq?tab=doc#Client) to put tasks on the queue.

```go
package main

import (
    "fmt"
    "log"
    "time"

    "github.com/hibiken/asynq"
    "your/app/package/tasks"
)

const redisAddr = "127.0.0.1:6379"

func main() {
    r := asynq.RedisClientOpt{Addr: redisAddr}
    c := asynq.NewClient(r)
    defer c.Close()

    // ------------------------------------------------------
    // Example 1: Enqueue task to be processed immediately.
    //            Use (*Client).Enqueue method.
    // ------------------------------------------------------

    t := tasks.NewEmailDeliveryTask(42, "some:template:id")
    res, err := c.Enqueue(t)
    if err != nil {
        log.Fatal("could not enqueue task: %v", err)
    }
    fmt.Printf("Enqueued Result: %+v\n", res)


    // ------------------------------------------------------------
    // Example 2: Schedule task to be processed in the future.
    //            Use ProcessIn or ProcessAt option.
    // ------------------------------------------------------------

    t = tasks.NewEmailDeliveryTask(42, "other:template:id")
    res, err = c.Enqueue(t, asynq.ProcessIn(24*time.Hour))
    if err != nil {
        log.Fatal("could not schedule task: %v", err)
    }
    fmt.Printf("Enqueued Result: %+v\n", res)


    // ----------------------------------------------------------------------------
    // Example 3: Set other options to tune task processing behavior.
    //            Options include MaxRetry, Queue, Timeout, Deadline, Unique etc.
    // ----------------------------------------------------------------------------

    c.SetDefaultOptions(tasks.TypeImageResize, asynq.MaxRetry(10), asynq.Timeout(3*time.Minute))

    t = tasks.NewImageResizeTask("some/blobstore/path")
    res, err = c.Enqueue(t)
    if err != nil {
        log.Fatal("could not enqueue task: %v", err)
    }
    fmt.Printf("Enqueued Result: %+v\n", res)

    // ---------------------------------------------------------------------------
    // Example 4: Pass options to tune task processing behavior at enqueue time.
    //            Options passed at enqueue time override default ones, if any.
    // ---------------------------------------------------------------------------

    t = tasks.NewImageResizeTask("some/blobstore/path")
    res, err = c.Enqueue(t, asynq.Queue("critical"), asynq.Timeout(30*time.Second))
    if err != nil {
        log.Fatal("could not enqueue task: %v", err)
    }
    fmt.Printf("Enqueued Result: %+v\n", res)
}
```

Next, start a worker server to process these tasks in the background.  
To start the background workers, use [`Server`](https://pkg.go.dev/github.com/hibiken/asynq?tab=doc#Server) and provide your [`Handler`](https://pkg.go.dev/github.com/hibiken/asynq?tab=doc#Handler) to process the tasks.

You can optionally use [`ServeMux`](https://pkg.go.dev/github.com/hibiken/asynq?tab=doc#ServeMux) to create a handler, just as you would with [`"net/http"`](https://golang.org/pkg/net/http/) Handler.

```go
package main

import (
    "log"

    "github.com/hibiken/asynq"
    "your/app/package/tasks"
)

const redisAddr = "127.0.0.1:6379"

func main() {
    r := asynq.RedisClientOpt{Addr: redisAddr}

    srv := asynq.NewServer(r, asynq.Config{
        // Specify how many concurrent workers to use
        Concurrency: 10,
        // Optionally specify multiple queues with different priority.
        Queues: map[string]int{
            "critical": 6,
            "default":  3,
            "low":      1,
        },
        // See the godoc for other configuration options
    })

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

For a more detailed walk-through of the library, see our [Getting Started Guide](https://github.com/hibiken/asynq/wiki/Getting-Started).

To Learn more about `asynq` features and APIs, see our [Wiki](https://github.com/hibiken/asynq/wiki) and [godoc](https://godoc.org/github.com/hibiken/asynq).

## Web UI

[Asynqmon](https://github.com/hibiken/asynqmon) is a web based tool for monitoring and administrating Asynq queues and tasks.
Please see the tool's [README](https://github.com/hibiken/asynqmon) for details.

Here's a few screenshots of the web UI.

**Queues view**  
![Web UI QueuesView](/docs/assets/asynqmon-queues-view.png)

**Tasks view**  
![Web UI TasksView](/docs/assets/asynqmon-task-view.png)

## Command Line Tool

Asynq ships with a command line tool to inspect the state of queues and tasks.

Here's an example of running the `stats` command.

![Gif](/docs/assets/demo.gif)

For details on how to use the tool, refer to the tool's [README](/tools/asynq/README.md).

## Installation

To install `asynq` library, run the following command:

```sh
go get -u github.com/hibiken/asynq
```

To install the CLI tool, run the following command:

```sh
go get -u github.com/hibiken/asynq/tools/asynq
```

## Requirements

| Dependency                 | Version |
| -------------------------- | ------- |
| [Redis](https://redis.io/) | v4.0+   |
| [Go](https://golang.org/)  | v1.13+  |

## Contributing

We are open to, and grateful for, any contributions (Github issues/pull-requests, feedback on Gitter channel, etc) made by the community.
Please see the [Contribution Guide](/CONTRIBUTING.md) before contributing.

## Acknowledgements

- [Sidekiq](https://github.com/mperham/sidekiq) : Many of the design ideas are taken from sidekiq and its Web UI
- [RQ](https://github.com/rq/rq) : Client APIs are inspired by rq library.
- [Cobra](https://github.com/spf13/cobra) : Asynq CLI is built with cobra

## License

Asynq is released under the MIT license. See [LICENSE](https://github.com/hibiken/asynq/blob/master/LICENSE).
