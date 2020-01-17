# Asynq

[![Build Status](https://travis-ci.com/hibiken/asynq.svg?token=paqzfpSkF4p23s5Ux39b&branch=master)](https://travis-ci.com/hibiken/asynq)&nbsp;[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](https://opensource.org/licenses/MIT)&nbsp;[![Go Report Card](https://goreportcard.com/badge/github.com/hibiken/asynq)](https://goreportcard.com/report/github.com/hibiken/asynq)&nbsp;[![GoDoc](https://godoc.org/github.com/hibiken/asynq?status.svg)](https://godoc.org/github.com/hibiken/asynq)

Simple and efficent asynchronous task processing library in Go.

**Important Note**: Current major version is zero (v0.x.x) to accomodate rapid development and fast iteration while getting early feedback from users. The public API could change without a major version update before the release of verson 1.0.0.

## Table of Contents

- [Overview](#overview)
- [Requirements](#requirements)
- [Installation](#installation)
- [Getting Started](#getting-started)
- [Monitoring CLI](#monitoring-cli)
- [Acknowledgements](#acknowledgements)
- [License](#license)

## Overview

Asynq provides a simple interface to asynchronous task processing.

Asynq also ships with a CLI to monitor the queues and take manual actions if needed.

Asynq provides:

- Clear separation of task producer and consumer
- Ability to schedule task processing in the future
- Automatic retry of failed tasks with exponential backoff
- Ability to configure max retry count per task
- Ability to configure max number of worker goroutines to process tasks
- Unix signal handling to safely shutdown background processing
- CLI to query and mutate queues state for mointoring and administrative purposes

## Requirements

| Dependency                 | Version |
| -------------------------- | ------- |
| [Redis](https://redis.io/) | v2.8+   |
| [Go](https://golang.org/)  | v1.12+  |

## Installation

```
go get -u github.com/hibiken/asynq
```

## Getting Started

1. Import `asynq` in your file.

```go
import "github.com/hibiken/asynq"
```

2. Use one of `RedisConnOpt` types to specify how to connect to Redis.

```go
var redis = &asynq.RedisClientOpt{
    Addr: "localhost:6379",
    // Omit if no password is required
    Password: "mypassword",
    // Use a dedicated db number for asynq.
    // By default, Redis offers 16 databases (0..15)
    DB: 0,
}
```

3. Create a `Client` instance to create and schedule tasks.

```go
func main() {
    client := asynq.NewClient(redis)

    // Create a task with typename and payload.
    t1 := asynq.NewTask(
        "send_welcome_email",
        map[string]interface{}{"user_id": 42})

    t2 := asynq.NewTask(
        "send_reminder_email",
        map[string]interface{}{"user_id": 42})

    // Process the task immediately.
    err := client.Schedule(t1, time.Now())

    // Process the task 24 hours later.
    err = client.Schedule(t2, time.Now().Add(24 * time.Hour))

    // Specify the max number of retry (default: 25)
    err = client.Schedule(t1, time.Now(), asynq.MaxRetry(1))
}
```

4. Create a `Background` instance to process tasks.

```go
func main() {
    bg := asynq.NewBackground(redis, &asynq.Config{
        Concurrency: 10,
    })

    // Blocks until signal TERM or INT is received.
    // For graceful shutdown, send signal TSTP to stop processing more tasks
    // before sending TERM or INT signal to terminate the process.
    bg.Run(handler)
}
```

Note that `Client` and `Background` are intended to be used in separate executable binaries.

The argument to `(*asynq.Background).Run` is an interface `asynq.Handler` which has one method `ProcessTask`.

```go
// ProcessTask should return nil if the processing of a task
// is successful.
//
// If ProcessTask return a non-nil error or panics, the task
// will be retried.
type Handler interface {
    ProcessTask(*Task) error
}
```

The simplest way to implement a handler is to define a function with the same signature and use `asynq.HandlerFunc` adapter type when passing it to `Run`.

```go
func handler(t *asynq.Task) error {
    switch t.Type {
    case "send_welcome_email":
        id, err := t.Payload.GetInt("user_id")
        if err != nil {
            return err
        }
        fmt.Printf("Send Welcome Email to %d\n", id)

    // ... handle other types ...

    default:
        return fmt.Errorf("unexpected task type: %s", t.Type)
    }
    return nil
}

func main() {
    bg := asynq.NewBackground(redis, &asynq.Config{
        Concurrency: 10,
    })

    // Use asynq.HandlerFunc adapter for a handler function
    bg.Run(asynq.HandlerFunc(handler))
}
```

## Monitoring CLI

TODO(hibiken): Describe basic usage of `asynqmon` CLI

## Acknowledgements

- [Sidekiq](https://github.com/mperham/sidekiq) : Many of the design ideas are taken from sidekiq and its Web UI
- [Cobra](https://github.com/spf13/cobra) : Asynqmon CLI is built with cobra

## License

Asynq is released under the MIT license. See [LICENSE](https://github.com/hibiken/asynq/blob/master/LICENSE).
