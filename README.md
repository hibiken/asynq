# Asynq

[![Build Status](https://travis-ci.com/hibiken/asynq.svg?token=paqzfpSkF4p23s5Ux39b&branch=master)](https://travis-ci.com/hibiken/asynq)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Go Report Card](https://goreportcard.com/badge/github.com/hibiken/asynq)](https://goreportcard.com/report/github.com/hibiken/asynq)
[![GoDoc](https://godoc.org/github.com/hibiken/asynq?status.svg)](https://godoc.org/github.com/hibiken/asynq)
[![Gitter chat](https://badges.gitter.im/go-asynq/gitter.svg)](https://gitter.im/go-asynq/community)

Simple and efficient asynchronous task processing library in Go.

**Important Note**: Current major version is zero (v0.x.x) to accomodate rapid development and fast iteration while getting early feedback from users. The public API could change without a major version update before v1.0.0 release.

## Table of Contents

- [Overview](#overview)
- [Requirements](#requirements)
- [Installation](#installation)
- [Getting Started](#getting-started)
- [Command Line Tool](#command-line-tool)
- [Acknowledgements](#acknowledgements)
- [License](#license)

## Overview

![Gif](/docs/assets/asynqmon_stats.gif)

Package asynq provides a framework for asynchronous task processing.

Asynq provides:

- Clear separation of task producer and consumer
- Ability to process multiple tasks concurrently
- Ability to schedule task processing in the future
- Automatic retry of failed tasks with exponential backoff
- [Ability to configure](https://github.com/hibiken/asynq/wiki/Task-Retry) task retry count and retry delay
- Support for [priority queues](https://github.com/hibiken/asynq/wiki/Priority-Queues)
- [Unix signal handling](https://github.com/hibiken/asynq/wiki/Signals) to gracefully shutdown background processing
- [Automatic failover](https://github.com/hibiken/asynq/wiki/Automatic-Failover) using Redis sentinels
- [Command line tool](/tools/asynqmon/README.md) to query tasks for monitoring and troubleshooting purposes

## Requirements

| Dependency                 | Version |
| -------------------------- | ------- |
| [Redis](https://redis.io/) | v2.8+   |
| [Go](https://golang.org/)  | v1.12+  |

## Installation

To install both `asynq` library and `asynqmon` command line tool, run the following command:

```
go get -u github.com/hibiken/asynq
go get -u github.com/hibiken/asynq/tools/asynqmon
```

## Getting Started

In this quick tour of `asynq`, we are going to create two programs.

- `producer.go` will create and schedule tasks to be processed asynchronously by the consumer.
- `consumer.go` will process the tasks created by the producer.

**This guide assumes that you are running a Redis server at `localhost:6379`**.
Before we start, make sure you have Redis installed and running.

The first thing we need to do is create two main files.

```sh
mkdir producer consumer
touch producer/producer.go consumer/consumer.go
```

Import `asynq` in both files.

```go
import "github.com/hibiken/asynq"
```

Asynq uses Redis as a message broker.
Use one of `RedisConnOpt` types to specify how to connect to Redis.
We are going to use `RedisClientOpt` here.

```go
// both in producer.go and consumer.go
var redis = &asynq.RedisClientOpt{
    Addr: "localhost:6379",
    // Omit if no password is required
    Password: "mypassword",
    // Use a dedicated db number for asynq.
    // By default, Redis offers 16 databases (0..15)
    DB: 0,
}
```

In `producer.go`, we are going to create a `Client` instance to create and schedule tasks.

In `asynq`, a unit of work to be performed is encapsluated in a struct called `Task`.
Which has two fields: `Type` and `Payload`.

```go
// Task represents a task to be performed.
type Task struct {
    // Type indicates the type of task to be performed.
    Type string

    // Payload holds data needed to perform the task.
    Payload Payload
}
```

To create a task, use `NewTask` function and pass type and payload for the task.

You schedule a task by calling `Client.Schedule` passing in the task and the timethe task neeeds to be processed.

```go
// producer.go
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
    if err != nil {
        log.Fatal(err)
    }

    // Process the task 24 hours later.
    err = client.Schedule(t2, time.Now().Add(24 * time.Hour))
    if err != nil {
        log.Fatal(err)
    }
}
```

In `consumer.go`, create a `Background` instance to process the tasks.

`NewBackground` function takes `RedisConnOpt` and `Config`.

You can take a look at documentation on `Config` to see the available options.

We are only going to specify the concurrency in this example.

```go
// consumer.go
func main() {
    bg := asynq.NewBackground(redis, &asynq.Config{
        Concurrency: 10,
    })

    bg.Run(handler)
}
```

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
        fmt.Printf("Send Welcome Email to User %d\n", id)

    case "send_reminder_email":
        id, err := t.Payload.GetInt("user_id")
        if err != nil {
            return err
        }
        fmt.Printf("Send Reminder Email to User %d\n", id)

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

We could keep adding cases to this handler function, but in a realistic application, it's convenient to define the logic for each case in a separate function.

To refactor our code, let's create a simple dispatcher which maps task type to its handler.

```go
// consumer.go

// Dispatcher is used to dispatch tasks to registered handlers.
type Dispatcher struct {
    mapping map[string]asynq.HandlerFunc
}

// HandleFunc registers a task handler
func (d *Dispatcher) HandleFunc(taskType string, fn asynq.HandlerFunc) {
    d.mapping[taskType] = fn
}

// ProcessTask processes a task.
//
// NOTE: Dispatcher satisfies asynq.Handler interface.
func (d *Dispatcher) ProcessTask(task *asynq.Task) error {
    fn, ok := d.mapping[task.Type]
    if !ok {
        return fmt.Errorf("no handler registered for %q", task.Type)
    }
    return fn(task)
}

func main() {
    d := &Dispatcher{mapping: make(map[string]asynq.HandlerFunc)}
    d.HandleFunc("send_welcome_email", sendWelcomeEmail)
    d.HandleFunc("send_reminder_email", sendReminderEmail)

    bg := asynq.NewBackground(redis, &asynq.Config{
        Concurrency: 10,
    })
    bg.Run(d)
}

func sendWelcomeEmail(t *asynq.Task) error {
    id, err := t.Payload.GetInt("user_id")
    if err != nil {
        return err
    }
    fmt.Printf("Send Welcome Email to User %d\n", id)
    return nil
}

func sendReminderEmail(t *asynq.Task) error {
    id, err := t.Payload.GetInt("user_id")
    if err != nil {
        return err
    }
    fmt.Printf("Send Welcome Email to User %d\n", id)
    return nil
}
```

Now that we have both task producer and consumer, we can run both programs.

```sh
go run producer.go
```

This will create two tasks: One that should processed immediately and another to be processed 24 hours later.

Let's use `asynqmon` tool to inspect the tasks.

```sh
asynqmon stats
```

You should able to see that there's one task in **Enqueued** state and another in **Scheduled** state.

Note: To understand the meaning of each state, see [Life of a Task](https://github.com/hibiken/asynq/wiki/Life-of-a-Task) on our Wiki page.

Let's run `asynqmon` with `watch` command so that we can continuously run the command to see the changes.

```sh
watch -n 3 asynqmon stats # Runs `asynqmon stats` every 3 seconds
```

And finally, let's start the consumer program to process scheduled tasks.

```sh
go run consumer.go
```

**Note**: This will not exit until you send a signal to terminate the program. See [Signal Wiki page](https://github.com/hibiken/asynq/wiki/Signals) for best practice on how to safely terminate background processing.

You should be able to see text printed in your terminal indicating that the task was processed successfully.

This was a whirlwind tour of `asynq` basics. To learn more about all of its features such as **[priority queues](https://github.com/hibiken/asynq/wiki/Priority-Queues)** and **[custom retry](https://github.com/hibiken/asynq/wiki/Task-Retry)**, see our [Wiki page](https://github.com/hibiken/asynq/wiki).

## Command Line Tool

Asynq ships with a command line tool to inspect the state of queues and tasks.

To install, run the following command:

    go get github.com/hibiken/asynq/tools/asynqmon

For details on how to use the tool, refer to the tool's [README](/tools/asynqmon/README.md).

## Acknowledgements

- [Sidekiq](https://github.com/mperham/sidekiq) : Many of the design ideas are taken from sidekiq and its Web UI
- [Cobra](https://github.com/spf13/cobra) : Asynqmon CLI is built with cobra

## License

Asynq is released under the MIT license. See [LICENSE](https://github.com/hibiken/asynq/blob/master/LICENSE).
