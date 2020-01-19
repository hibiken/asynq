# Asynqmon

Asynqmon is a CLI tool to monitor the queues managed by `asynq` package.

In order to use the tool, compile it using the following command:

    go get github.com/hibiken/asynq/tools/asynqmon

This will create the asynqmon executable under your `$GOPATH/bin` directory.

## Quick Start

Asynqmon tool has a few commands to inspect the state of tasks and queues.

Run `asynqmon help` to see all the available commands.

Asynqmon needs to connect to a redis-server to inspect the state of queues and tasks. Use flags to specify the options to connect to the redis-server used by your application.

By default, Asynqmon will try to connect to a redis server running at `localhost:6379`.

### Stats

Stats command gives the overview of the current state of tasks and queues. Run it in conjunction with `watch` command to repeatedly run `stats`.

Example:

    watch -n 3 asynqmon stats

This will run `asynqmon stats` command every 3 seconds.

![Gif](/docs/assets/asynqmon_stats.gif)
