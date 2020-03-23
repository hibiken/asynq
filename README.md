# Asynq

[![Build Status](https://travis-ci.com/hibiken/asynq.svg?token=paqzfpSkF4p23s5Ux39b&branch=master)](https://travis-ci.com/hibiken/asynq)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Go Report Card](https://goreportcard.com/badge/github.com/hibiken/asynq)](https://goreportcard.com/report/github.com/hibiken/asynq)
[![GoDoc](https://godoc.org/github.com/hibiken/asynq?status.svg)](https://godoc.org/github.com/hibiken/asynq)
[![Gitter chat](https://badges.gitter.im/go-asynq/gitter.svg)](https://gitter.im/go-asynq/community)
[![codecov](https://codecov.io/gh/hibiken/asynq/branch/master/graph/badge.svg)](https://codecov.io/gh/hibiken/asynq)

Asynq is a simple Go library for queueing tasks and processing them in the background with workers.  
It is backed by Redis and it is designed to have a low barrier to entry. It should be integrated in your web stack easily.

**Important Note**: Current major version is zero (v0.x.x) to accomodate rapid development and fast iteration while getting early feedback from users. The public API could change without a major version update before v1.0.0 release.

![Task Queue Diagram](/docs/assets/task-queue.png)

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

// A list of background task types.
const (
    EmailDelivery   = "email:deliver"
    ImageProcessing = "image:process"
)

// Write function NewXXXTask to create a task.

func NewEmailDeliveryTask(userID int, tmplID string) *asynq.Task {
    payload := map[string]interface{}{"user_id": userID, "template_id": tmplID}
    return asynq.NewTask(EmailDelivery, payload)
}

func NewImageProcessingTask(src, dst string) *asynq.Task {
    payload := map[string]interface{}{"src": src, "dst": dst}
    return asynq.NewTask(ImageProcessing, payload)
}

// Write function HandleXXXTask to handle the given task.
// NOTE: It satisfies the asynq.HandlerFunc interface.

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
    // Email delivery logic ...
    return nil
}

func HandleImageProcessingTask(ctx context.Context, t *asynq.Task) error {
    src, err := t.Payload.GetString("src")
    if err != nil {
        return err
    }
    dst, err := t.Payload.GetString("dst")
    if err != nil {
        return err
    }
    fmt.Printf("Process image: src = %s, dst = %s\n", src, dst)
    // Image processing logic ...
    return nil
}
```

In your web application code, import the above package and use [`Client`](https://pkg.go.dev/github.com/hibiken/asynq?tab=doc#Client) to enqueue tasks to the task queue.  
A task will be processed by a background worker as soon as the task gets enqueued.  
Scheduled tasks will be stored in Redis and will be enqueued at the specified time.

```go
package main

import (
    "time"

    "github.com/hibiken/asynq"
    "your/app/package/tasks"
)

const redisAddr = "127.0.0.1:6379"

func main() {
    r := &asynq.RedisClientOpt{Addr: redisAddr}
    c := asynq.NewClient(r)

    // Example 1: Enqueue task to be processed immediately.

    t := tasks.NewEmailDeliveryTask(42, "some:template:id")
    err := c.Enqueue(t)
    if err != nil {
        log.Fatal("could not enqueue task: %v", err)
    }


    // Example 2: Schedule task to be processed in the future.

    t = tasks.NewEmailDeliveryTask(42, "other:template:id")
    err = c.EnqueueIn(24*time.Hour, t)
    if err != nil {
        log.Fatal("could not schedule task: %v", err)
    }


    // Example 3: Pass options to tune task processing behavior.
    // Options include MaxRetry, Queue, Timeout, Deadline, etc.

    t = tasks.NewImageProcessingTask("some/blobstore/url", "other/blobstore/url")
    err = c.Enqueue(t, asynq.MaxRetry(10), asynq.Queue("critical"), asynq.Timeout(time.Minute))
    if err != nil {
        log.Fatal("could not enqueue task: %v", err)
    }
}
```

Next, create a binary to process these tasks in the background.  
To start the background workers, use [`Background`](https://pkg.go.dev/github.com/hibiken/asynq?tab=doc#Background) and provide your [`Handler`](https://pkg.go.dev/github.com/hibiken/asynq?tab=doc#Handler) to process the tasks.

You can optionally use [`ServeMux`](https://pkg.go.dev/github.com/hibiken/asynq?tab=doc#ServeMux) to create a handler, just as you would with [`"net/http"`](https://golang.org/pkg/net/http/) Handler.

```go
package main

import (
    "github.com/hibiken/asynq"
    "your/app/package/tasks"
)

const redisAddr = "127.0.0.1:6379"

func main() {
    r := &asynq.RedisClientOpt{Addr: redisAddr}

    bg := asynq.NewBackground(r, &asynq.Config{
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
    mux.HandleFunc(tasks.EmailDelivery, tasks.HandleEmailDeliveryTask)
    mux.HandleFunc(tasks.ImageProcessing, tasks.HandleImageProcessingTask)
    // ...register other handlers...

    bg.Run(mux)
}
```

For a more detailed walk-through of the library, see our [Getting Started Guide](https://github.com/hibiken/asynq/wiki/Getting-Started).

To Learn more about `asynq` features and APIs, see our [Wiki](https://github.com/hibiken/asynq/wiki) and [godoc](https://godoc.org/github.com/hibiken/asynq).

## Command Line Tool

Asynq ships with a command line tool to inspect the state of queues and tasks.

Here's an example of running the `stats` command.

![Gif](/docs/assets/demo.gif)

For details on how to use the tool, refer to the tool's [README](/tools/asynqmon/README.md).

## Installation

To install `asynq` library, run the following command:

```sh
go get -u github.com/hibiken/asynq
```

To install the CLI tool, run the following command:

```sh
go get -u github.com/hibiken/asynq/tools/asynqmon
```

## Requirements

| Dependency                 | Version |
| -------------------------- | ------- |
| [Redis](https://redis.io/) | v2.8+   |
| [Go](https://golang.org/)  | v1.13+  |

## Contributing

We are open to, and grateful for, any contributions (Github issues/pull-requests, feedback on Gitter channel, etc) made by the community.
Please see the [Contribution Guide](/CONTRIBUTING.md) before contributing.

## Acknowledgements

- [Sidekiq](https://github.com/mperham/sidekiq) : Many of the design ideas are taken from sidekiq and its Web UI
- [RQ](https://github.com/rq/rq) : Client APIs are inspired by rq library.
- [Cobra](https://github.com/spf13/cobra) : Asynqmon CLI is built with cobra

## License

Asynq is released under the MIT license. See [LICENSE](https://github.com/hibiken/asynq/blob/master/LICENSE).
