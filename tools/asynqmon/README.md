# Asynqmon

Asynqmon is a command line tool to monitor the tasks managed by `asynq` package.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
  - [Stats](#stats)
  - [History](#history)
  - [Process Status](#process-status)
  - [List](#list)
  - [Enqueue](#enqueue)
  - [Delete](#delete)
  - [Kill](#kill)
- [Config File](#config-file)

## Installation

In order to use the tool, compile it using the following command:

    go get github.com/hibiken/asynq/tools/asynqmon

This will create the asynqmon executable under your `$GOPATH/bin` directory.

## Quickstart

The tool has a few commands to inspect the state of tasks and queues.

Run `asynqmon help` to see all the available commands.

Asynqmon needs to connect to a redis-server to inspect the state of queues and tasks. Use flags to specify the options to connect to the redis-server used by your application.

By default, Asynqmon will try to connect to a redis server running at `localhost:6379`.

### Stats

Stats command gives the overview of the current state of tasks and queues. You can run it in conjunction with `watch` command to repeatedly run `stats`.

Example:

    watch -n 3 asynqmon stats

This will run `asynqmon stats` command every 3 seconds.

![Gif](/docs/assets/asynqmon_stats.gif)

### History

History command shows the number of processed and failed tasks from the last x days.

By default, it shows the stats from the last 10 days. Use `--days` to specify the number of days.

Example:

    asynqmon history --days=30

![Gif](/docs/assets/asynqmon_history.gif)

### Process Status

PS (ProcessStatus) command shows the list of running worker processes.

Example:

    asynqmon ps

![Gif](/docs/assets/asynqmon_ps.gif)

### List

List command shows all tasks in the specified state in a table format

Example:

    asynqmon ls retry
    asynqmon ls scheduled
    asynqmon ls dead
    asynqmon ls enqueued:default
    asynqmon ls inprogress

### Enqueue

There are two commands to enqueue tasks.

Command `enq` takes a task ID and moves the task to **Enqueued** state. You can obtain the task ID by running `ls` command.

Example:

    asynqmon enq d:1575732274:bnogo8gt6toe23vhef0g

Command `enqall` moves all tasks to **Enqueued** state from the specified state.

Example:

    asynqmon enqall retry

Running the above command will move all **Retry** tasks to **Enqueued** state.

### Delete

There are two commands for task deletion.

Command `del` takes a task ID and deletes the task. You can obtain the task ID by running `ls` command.

Example:

    asynqmon del r:1575732274:bnogo8gt6toe23vhef0g

Command `delall` deletes all tasks which are in the specified state.

Example:

    asynqmon delall retry

Running the above command will delete all **Retry** tasks.

### Kill

There are two commands to kill (i.e. move to dead state) tasks.

Command `kill` takes a task ID and kills the task. You can obtain the task ID by running `ls` command.

Example:

    asynqmon kill r:1575732274:bnogo8gt6toe23vhef0g

Command `killall` kills all tasks which are in the specified state.

Example:

    asynqmon killall retry

Running the above command will move all **Retry** tasks to **Dead** state.

## Config File

You can use a config file to set default values for the flags.
This is useful, for example when you have to connect to a remote redis server.

By default, `asynqmon` will try to read config file located in
`$HOME/.asynqmon.(yaml|json)`. You can specify the file location via `--config` flag.

Config file example:

```yaml
uri: 127.0.0.1:6379
db: 2
password: mypassword
```

This will set the default values for `--uri`, `--db`, and `--password` flags.
