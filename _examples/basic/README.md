# Basic asynq flow

A client enqueues tasks, a worker processes them, Redis sits in between.

## 1. Start Redis

Needs Redis (or Valkey) on `localhost:6379`. Use whatever you have:

```sh
docker run -d -p 6379:6379 redis:7   # or: redis-server / valkey-server
```

Check it: `redis-cli ping` should print `PONG`.

## 2. Run it

```sh
go run ./_examples/basic/worker   # terminal 1
go run ./_examples/basic/client   # terminal 2
```

The worker logs the tasks as it processes them. Ctrl-C to stop.
