# Changelog

All notable changes to this project will be documented in this file.

The format is based on ["Keep a Changelog"](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- NewTask constructor
- `Queues` option in `Config` to specify mutiple queues with priority level
- `Client` can schedule a task with `asynq.Queue(name)` to specify which queue to use
- `StrictPriority` option in `Config` to specify whether the priority should be followed strictly
- [CLI] `asynqmon rmq` command to remove queue

### Changed

- [CLI] `asynqmon stats` now shows the total of all enqueued tasks under "Enqueued"
- [CLI] `asynqmon stats` now shows each queue's task count
- Task type is now immutable (i.e., Payload is read-only)

## [0.1.0] - 2020-01-04

### Added

- Initial version of asynq package
- Initial version of asynqmon CLI
