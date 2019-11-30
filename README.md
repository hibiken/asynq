# Asynq&nbsp;[![Build Status](https://travis-ci.com/hibiken/asynq.svg?token=paqzfpSkF4p23s5Ux39b&branch=master)](https://travis-ci.com/hibiken/asynq)

Asynq is a simple asynchronous task queue library in Go.

## Requirements

- Redis: 2.6+
- Go: 1.12+

## Installation

```
go get -u github.com/hibiken/asynq
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

    t1 := &asynq.Task{
        Type: "send_welcome_email",
        Payload: map[string]interface{
          "recipient_id": 1234,
        },
    }

    t2 := &asynq.Task{
        Type: "send_reminder_email",
        Payload: map[string]interface{
          "recipient_id": 1234,
        },
    }

    // send welcome email now.
    client.Process(t1, time.Now())

    // send reminder email 24 hours later.
    client.Process(t2, time.Now().Add(24 * time.Hour))
}
```

3. Create a `Background` instance to process tasks.

```go
func main() {
    bg := asynq.NewBackground(10, &asynq.RedisOpt{
        Addr: "localhost:6379",
    })

    bg.Run(handler)
}

func handler(t *Task) error {
    switch t.Type {
        case "send_welcome_email":
            rid, ok := t.Payload["recipient_id"]
            if !ok {
                return fmt.Errorf("recipient_id not found in payload")
            }
            fmt.Printf("Send Welcome Email to %d\n", rid.(int))

        // ... handle other task types.

        default:
            return fmt.Errorf("unexpected task type: %s", t.Type)
    }
    return nil
}
```
