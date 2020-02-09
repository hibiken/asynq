# Asynq

[![Build Status](https://travis-ci.com/hibiken/asynq.svg?token=paqzfpSkF4p23s5Ux39b&branch=master)](https://travis-ci.com/hibiken/asynq)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Go Report Card](https://goreportcard.com/badge/github.com/hibiken/asynq)](https://goreportcard.com/report/github.com/hibiken/asynq)
[![GoDoc](https://godoc.org/github.com/hibiken/asynq?status.svg)](https://godoc.org/github.com/hibiken/asynq)
[![Gitter chat](https://badges.gitter.im/go-asynq/gitter.svg)](https://gitter.im/go-asynq/community)

Asynq is a simple Go library for queueing tasks and processing them in the background with workers.  
It is backed by Redis and it is designed to have a low barrier to entry. It should be integrated in your web stack easily.

**Important Note**: Current major version is zero (v0.x.x) to accomodate rapid development and fast iteration while getting early feedback from users. The public API could change without a major version update before v1.0.0 release.

[Quick Tour of Asynq (YouTube video)](https://www.youtube.com/watch?v=FzMJdddNcmA&t=62s)

![Gif](/docs/assets/demo.gif)

## Installation

To install `asynq` library, run the following command:

```sh
go get -u github.com/hibiken/asynq
```

## Quickstart

First, make sure you are running a Redis server locally.

```sh
$ redis-server
```

To create and schedule tasks, use `Client` and provide a task and when to process the task.

```go
func main() {
    r := &asynq.RedisClientOpt{
        Addr: "localhost:6379",
    }

    client := asynq.NewClient(r)

    // Create a task with task type and payload
    t1 := asynq.NewTask("send_welcome_email", map[string]interface{}{"user_id": 42})

    t2 := asynq.NewTask("send_reminder_email", map[string]interface{}{"user_id": 42})

    // Process immediately
    err := client.Schedule(t1, time.Now())

    // Process 24 hrs later
    err = client.Schedule(t2, time.Now().Add(24 * time.Hour))

    // If processing fails, retry up to 10 times (Default is 25)
    err = client.Schedule(t1, time.Now(), asynq.Retry(10))

    // Use custom queue called "critical"
    err = client.Schedule(t1, time.Now(), asynq.Queue("critical"))
}
```

To start the background workers, use `Background` and provide your `Handler` to process the tasks.

```go
func main() {
    r := &asynq.RedisClientOpt{
        Addr: "localhost:6379",
    }

    bg := asynq.NewBackground(r, &asynq.Config{
        // Specify how many concurrent workers to use
        Concurrency: 10,
        // You can optionally create multiple queues
        // with different priority level
        Queues: map[string]uint{
            "critical": 6,
            "default":  3,
            "low":      1,
        },
        // See the godoc for other configuration options
    })

    bg.Run(handler)
}
```

`Handler` is an interface with one method `ProcessTask` with the following signature.

```go
// ProcessTask should return nil if the processing of a task
// is successful.
//
// If ProcessTask return a non-nil error or panics, the task
// will be retried after delay.
type Handler interface {
    ProcessTask(*Task) error
}
```

For a more detailed walk-through of the library, see our [Getting Started Guide](https://github.com/hibiken/asynq/wiki/Getting-Started).

To Learn more about `asynq` features and APIs, see our [Wiki pages](https://github.com/hibiken/asynq/wiki) and [godoc](https://godoc.org/github.com/hibiken/asynq).

## Requirements

| Dependency                 | Version |
| -------------------------- | ------- |
| [Redis](https://redis.io/) | v2.8+   |
| [Go](https://golang.org/)  | v1.12+  |

## Command Line Tool

Asynq ships with a command line tool to inspect the state of queues and tasks.

To install, run the following command:

```sh
go get -u github.com/hibiken/asynq/tools/asynqmon
```

For details on how to use the tool, refer to the tool's [README](/tools/asynqmon/README.md).

## Contributing

We are open to, and grateful for, any contributions (Github issues/pull-requests, feedback on Gitter channel, etc) made by the community.
Please see the [Contribution Guide](/CONTRIBUTING.md) before contributing.

## Acknowledgements

- [Sidekiq](https://github.com/mperham/sidekiq) : Many of the design ideas are taken from sidekiq and its Web UI
- [Cobra](https://github.com/spf13/cobra) : Asynqmon CLI is built with cobra

## License

Asynq is released under the MIT license. See [LICENSE](https://github.com/hibiken/asynq/blob/master/LICENSE).
