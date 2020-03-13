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

To create and schedule tasks, use `Client` and provide a task and when to enqueue the task.  
A task will be processed by a background worker as soon as the task gets enqueued.  
Scheduled tasks will be stored in Redis and will be enqueued at the specified time.  

```go
func main() {
    r := &asynq.RedisClientOpt{
        Addr: "127.0.0.1:6379",
    }

    c := asynq.NewClient(r)

    // Example 1: Enqueue task to be processed immediately.

    t := asynq.NewTask("email:signup", map[string]interface{}{"user_id": 42})
    err := c.Enqueue(t)
    if err != nil {
        log.Fatal("could not enqueue task: %v", err)
    } 


    // Example 2: Schedule task to be processed in the future.

    t = asynq.NewTask("email:reminder", map[string]interface{}{"user_id": 42})
    err = c.EnqueueIn(24*time.Hour, t)
    if err != nil {
        log.Fatal("could not schedule task: %v", err)
    }


    // Example 3: Pass options to tune task processing behavior. 
    // Options include MaxRetry, Queue, Timeout, Deadline, etc.

    t = asynq.NewTask("email:reminder", map[string]interface{}{"user_id": 42})
    err = c.Enqueue(t, asynq.MaxRetry(10), asynq.Queue("critical"), asynq.Timeout(time.Minute))
    if err != nil {
        log.Fatal("could not enqueue task: %v", err)
    }
}
```

To start the background workers, use `Background` and provide your `Handler` to process the tasks.

`Handler` is an interface with one method `ProcessTask` with the following signature.

```go
// ProcessTask should return nil if the processing of a task is successful.
//
// If ProcessTask return a non-nil error or panics, the task will be retried after delay.
type Handler interface {
    ProcessTask(context.Context, *asynq.Task) error
}
```

You can optionally use `ServeMux` to create a handler, just as you would with `"net/http"` Handler.

```go
func main() {
    r := &asynq.RedisClientOpt{
        Addr: "127.0.0.1:6379",
    }

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
    mux.HandleFunc("email:signup", signupEmailHandler)
    mux.HandleFunc("email:reminder", reminderEmailHandler)
    // ...register other handlers...

    bg.Run(mux)
}

// function with the same signature as the sole method for the Handler interface.
func signupEmailHandler(ctx context.Context, t *asynq.Task) error {
    id, err := t.Payload.GetInt("user_id")
    if err != nil {
        return err
    }
    fmt.Printf("Send welcome email to user %d\n", id)
    // ...your email sending logic...
    return nil
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
| [Go](https://golang.org/)  | v1.12+  |

## Contributing

We are open to, and grateful for, any contributions (Github issues/pull-requests, feedback on Gitter channel, etc) made by the community.
Please see the [Contribution Guide](/CONTRIBUTING.md) before contributing.

## Acknowledgements

- [Sidekiq](https://github.com/mperham/sidekiq) : Many of the design ideas are taken from sidekiq and its Web UI
- [RQ](https://github.com/rq/rq) : Client APIs are inspired by rq library.
- [Cobra](https://github.com/spf13/cobra) : Asynqmon CLI is built with cobra

## License

Asynq is released under the MIT license. See [LICENSE](https://github.com/hibiken/asynq/blob/master/LICENSE).
