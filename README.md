# Asynq&nbsp;[![Build Status](https://travis-ci.com/hibiken/asynq.svg?token=paqzfpSkF4p23s5Ux39b&branch=master)](https://travis-ci.com/hibiken/asynq)

Simple, efficent asynchronous task processing library in Go.

## Table of Contents

- [Overview](#overview)
- [Requirements](#requirements)
- [Installation](#installation)
- [Getting Started](#getting-started)
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
- Enhanced reliability TODO(hibiken): link to wiki page describing this.
- CLI to query and mutate queues state for mointoring and administrative purposes

## Requirements

- Redis: 2.6+
- Go: 1.12+

## Installation

```
go get github.com/hibiken/asynq
```

## Getting Started

1. Import `asynq` in your file.

```go
import "github.com/hibiken/asynq"
```

2. Create a `Client` instance to create tasks.

```go
func main() {
    client := asynq.NewClient(&asynq.RedisOpt{
        Addr: "localhost:6379",
    })

    t1 := asynq.Task{
        Type: "send_welcome_email",
        Payload: map[string]interface{}{
          "recipient_id": 1234,
        },
    }

    t2 := asynq.Task{
        Type: "send_reminder_email",
        Payload: map[string]interface{}{
          "recipient_id": 1234,
        },
    }

    // process the task immediately.
    err := client.Process(&t1, time.Now())

    // process the task 24 hours later.
    err = client.Process(&t2, time.Now().Add(24 * time.Hour))

    // specify the max number of retry (default: 25)
    err = client.Process(&t1, time.Now(), asynq.MaxRetry(1))
}
```

3. Create a `Background` instance to process tasks.

```go
func main() {
    bg := asynq.NewBackground(10, &asynq.RedisOpt{
        Addr: "localhost:6379",
    })

    // Blocks until signal TERM or INT is received.
    // For graceful shutdown, send signal TSTP to stop processing more tasks
    // before sending TERM or INT signal.
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
        id, err := t.Payload.GetInt("recipient_id")
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
    bg := asynq.NewBackground(10, &asynq.RedisOpt{
        Addr: "localhost:6379",
    })

    // Use asynq.HandlerFunc adapter for a handler function
    bg.Run(asynq.HandlerFunc(handler))
}
```

## License

Asynq is released under the MIT license. See [LICENSE](https://github.com/hibiken/asynq/blob/master/LICENSE).
