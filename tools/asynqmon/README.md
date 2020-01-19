# Asynqmon

Asynqmon is a CLI tool to monitor the queues managed by `asynq` package.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
  - [Stats](#stats)
  - [History](#history)
  - [List](#list)
  - [Enqueue](#enqueue)
  - [Delete](#delete)
  - [Kill](#kill)
- [Config File](#config-file)

## Installation

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

### History

TODO: Add discription

### List

TODO: Add discription

### Enqueue

TODO: Add discription

### Delete

TODO: Add discription

### Kill

TODO: Add discription

## Config File

You can use a config file to set default values for flags.
This is useful, for example when you have to connect to a remote redis server.

By default, `asynqmon` will try to read config file located in
`$HOME/.asynqmon.(yml|json)`. You can specify the file location via `--config` flag.

Config file example:

```yml
uri: 127.0.0.1:6379
db: 2
password: mypassword
```

This will set the default values for `--uri`, `--db`, and `--password` flags.
