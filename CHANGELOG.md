# Changelog

All notable changes to this project will be documented in this file.

The format is based on ["Keep a Changelog"](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `SchedulerEntries` method is added to `Inspector`.
- `DeleteQueue` method is added to `Inspector`.

## [0.13.1] - 2020-11-22

### Fixed

- Fixed processor to wait for specified time duration before forcefully shutdown workers.


## [0.13.0] - 2020-10-13

### Added

- `Scheduler` type is added to enable periodic tasks. See the godoc for its APIs and [wiki](https://github.com/hibiken/asynq/wiki/Periodic-Tasks) for the getting-started guide.

### Changed

- interface `Option` has changed. See the godoc for the new interface.
  This change would have no impact as long as you are using exported functions (e.g. `MaxRetry`, `Queue`, etc)
  to create `Option`s.

### Added

- `Payload.String() string`  method is added
- `Payload.MarshalJSON() ([]byte, error)` method is added

## [0.12.0] - 2020-09-12

**IMPORTANT**: If you are upgrading from a previous version, please install the latest version of the CLI `go get -u github.com/hibiken/asynq/tools/asynq` and run `asynq migrate` command. No process should be writing to Redis while you run the migration command. 

## The semantics of queue have changed
Previously, we called tasks that are ready to be processed *"Enqueued tasks"*, and other tasks that are scheduled to be processed in the future *"Scheduled tasks"*, etc.
We changed the semantics of *"Enqueue"* slightly; All tasks that client pushes to Redis are *Enqueued* to a queue. Within a queue, tasks will transition from one state to another. 
Possible task states are:
- `Pending`: task is ready to be processed (previously called "Enqueued")
- `Active`: tasks is currently being processed (previously called "InProgress")
- `Scheduled`: task is scheduled to be processed in the future
- `Retry`: task failed to be processed and will be retried again in the future
- `Dead`: task has exhausted all of its retries and stored for manual inspection purpose

**These semantics change is reflected in the new `Inspector` API and CLI commands.**

---

### Changed
#### `Client`
Use `ProcessIn` or `ProcessAt` option to schedule a task instead of `EnqueueIn` or `EnqueueAt`.

| Previously                  | v0.12.0                                    |
|-----------------------------|--------------------------------------------|
| `client.EnqueueAt(t, task)` | `client.Enqueue(task, asynq.ProcessAt(t))` |  
| `client.EnqueueIn(d, task)` | `client.Enqueue(task, asynq.ProcessIn(d))` |  

#### `Inspector`
All Inspector methods are scoped to a queue, and the methods take `qname (string)` as the first argument.  
`EnqueuedTask` is renamed to `PendingTask` and its corresponding methods.  
`InProgressTask` is renamed to `ActiveTask` and its corresponding methods.  
Command "Enqueue" is replaced by the verb "Run" (e.g. `EnqueueAllScheduledTasks` --> `RunAllScheduledTasks`)

#### `CLI`
CLI commands are restructured to use subcommands. Commands are organized into a few management commands:
To view details on any command, use `asynq help <command> <subcommand>`.
- `asynq stats`
- `asynq queue [ls inspect history rm pause unpause]`
- `asynq task [ls cancel delete kill run delete-all kill-all run-all]`
- `asynq server [ls]`

### Added

#### `RedisConnOpt`
- `RedisClusterClientOpt` is added to connect to Redis Cluster.
- `Username` field is added to all `RedisConnOpt` types in order to authenticate connection when Redis ACLs are used.

#### `Client`
- `ProcessIn(d time.Duration) Option` and `ProcessAt(t time.Time) Option` are added to replace `EnqueueIn` and `EnqueueAt` functionality.

#### `Inspector`
- `Queues() ([]string, error)` method is added to get all queue names.
- `ClusterKeySlot(qname string) (int64, error)` method is added to get queue's hash slot in Redis cluster.
- `ClusterNodes(qname string) ([]ClusterNode, error)` method is added to get a list of Redis cluster nodes for the given queue. 
- `Close() error` method is added to close connection with redis.

### `Handler` 
- `GetQueueName(ctx context.Context) (string, bool)` helper is added to extract queue name from a context.

## [0.11.0] - 2020-07-28

### Added

- `Inspector` type was added to monitor and mutate state of queues and tasks.
- `HealthCheckFunc` and `HealthCheckInterval` fields were added to `Config` to allow user to specify a callback
  function to check for broker connection.

## [0.10.0] - 2020-07-06

### Changed

- All tasks now requires timeout or deadline. By default, timeout is set to 30 mins.
- Tasks that exceed its deadline are automatically retried.
- Encoding schema for task message has changed. Please install the latest CLI and run `migrate` command if 
  you have tasks enqueued with the previous version of asynq.
- API of `(*Client).Enqueue`, `(*Client).EnqueueIn`, and `(*Client).EnqueueAt` has changed to return a `*Result`.
- API of `ErrorHandler` has changed. It now takes context as the first argument and removed `retried`, `maxRetry` from the argument list.
  Use `GetRetryCount` and/or `GetMaxRetry` to get the count values.

## [0.9.4] - 2020-06-13

### Fixed

- Fixes issue of same tasks processed by more than one worker (https://github.com/hibiken/asynq/issues/90).

## [0.9.3] - 2020-06-12

### Fixed

- Fixes the JSON number overflow issue (https://github.com/hibiken/asynq/issues/166).


## [0.9.2] - 2020-06-08

### Added

- The `pause` and `unpause` commands were added to the CLI. See README for the CLI for details.

## [0.9.1] - 2020-05-29

### Added

- `GetTaskID`, `GetRetryCount`, and `GetMaxRetry` functions were added to extract task metadata from context.

## [0.9.0] - 2020-05-16

### Changed

- `Logger` interface has changed. Please see the godoc for the new interface.

### Added

- `LogLevel` type is added. Server's log level can be specified through `LogLevel` field in `Config`.

## [0.8.3] - 2020-05-08

### Added

- `Close` method is added to `Client`.

## [0.8.2] - 2020-05-03

### Fixed

- [Fixed cancelfunc leak](https://github.com/hibiken/asynq/pull/145)

## [0.8.1] - 2020-04-27

### Added

- `ParseRedisURI` helper function is added to create a `RedisConnOpt` from a URI string.
- `SetDefaultOptions` method is added to `Client`.

## [0.8.0] - 2020-04-19

### Changed

- `Background` type is renamed to `Server`.
- To upgrade from the previous version, Update `NewBackground` to `NewServer` and pass `Config` by value.
- CLI is renamed to `asynq`.
- To upgrade the CLI to the latest version run `go get -u github.com/hibiken/tools/asynq`
- The `ps` command in CLI is renamed to `servers`
- `Concurrency` defaults to the number of CPUs when unset or set to a negative value.

### Added

- `ShutdownTimeout` field is added to `Config` to speicfy timeout duration used during graceful shutdown.
- New `Server` type exposes `Start`, `Stop`, and `Quiet` as well as `Run`.

## [0.7.1] - 2020-04-05

### Fixed

- Fixed signal handling for windows.

## [0.7.0] - 2020-03-22

### Changed

- Support Go v1.13+, dropped support for go v1.12

### Added

- `Unique` option was added to allow client to enqueue a task only if it's unique within a certain time period.

## [0.6.2] - 2020-03-15

### Added

- `Use` method was added to `ServeMux` to apply middlewares to all handlers.

## [0.6.1] - 2020-03-12

### Added

- `Client` can optionally schedule task with `asynq.Deadline(time)` to specify deadline for task's context. Default is no deadline.
- `Logger` option was added to config, which allows user to specify the logger used by the background instance.

## [0.6.0] - 2020-03-01

### Added

- Added `ServeMux` type to make it easy for users to implement Handler interface.
- `ErrorHandler` type was added. Allow users to specify error handling function (e.g. Report error to error reporting service such as Honeybadger, Bugsnag, etc)

## [0.5.0] - 2020-02-23

### Changed

- `Client` API has changed. Use `Enqueue`, `EnqueueAt` and `EnqueueIn` to enqueue and schedule tasks.

### Added

- `asynqmon workers` was added to list all running workers information

## [0.4.0] - 2020-02-13

### Changed

- `Handler` interface has changed. `ProcessTask` method takes two arguments `context.Context` and `*asynq.Task`
- `Queues` field in `Config` has change from `map[string]uint` to `map[string]int`

### Added

- `Client` can optionally schedule task with `asynq.Timeout(duration)` to specify timeout duration for task. Default is no timeout.
- `asynqmon cancel [task id]` will send a cancelation signal to the goroutine processing the speicified task.

## [0.3.0] - 2020-02-04

### Added

- `asynqmon ps` was added to list all background worker processes

## [0.2.2] - 2020-01-26

### Fixed

- Fixed restoring unfinished tasks back to correct queues.

### Changed

- `asynqmon ls` command is now paginated (default 30 tasks from first page)
- `asynqmon ls enqueued:[queue name]` requires queue name to be specified

## [0.2.1] - 2020-01-22

### Fixed

- More structured log messages
- Prevent spamming logs with a bunch of errors when Redis connection is lost
- Fixed and updated README doc

## [0.2.0] - 2020-01-19

### Added

- NewTask constructor
- `Queues` option in `Config` to specify mutiple queues with priority level
- `Client` can schedule a task with `asynq.Queue(name)` to specify which queue to use
- `StrictPriority` option in `Config` to specify whether the priority should be followed strictly
- `RedisConnOpt` to abstract away redis client implementation
- [CLI] `asynqmon rmq` command to remove queue

### Changed

- `Client` and `Background` constructors take `RedisConnOpt` as their first argument.
- `asynqmon stats` now shows the total of all enqueued tasks under "Enqueued"
- `asynqmon stats` now shows each queue's task count
- `asynqmon history` now doesn't take any arguments and shows data from the last 10 days by default (use `--days` flag to change the number of days)
- Task type is now immutable (i.e., Payload is read-only)

## [0.1.0] - 2020-01-04

### Added

- Initial version of asynq package
- Initial version of asynqmon CLI
