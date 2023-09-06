# Asynq CLI

Asynq CLI is a command line tool to monitor the queues and tasks managed by `asynq` package.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Config File](#config-file)

## Installation

In order to use the tool, compile it using the following command:

    go install github.com/Kua-Fu/asynq/tools/asynq

This will create the asynq executable under your `$GOPATH/bin` directory.

## Usage

### Commands

To view details on any command, use `asynq help <command> <subcommand>`.

- `asynq dash`
- `asynq stats`
- `asynq queue [ls inspect history rm pause unpause]`
- `asynq task [ls cancel delete archive run delete-all archive-all run-all]`
- `asynq server [ls]`

### Global flags

Asynq CLI needs to connect to a redis-server to inspect the state of queues and tasks. Use flags to specify the options to connect to the redis-server used by your application.
To connect to a redis cluster, pass `--cluster` and `--cluster_addrs` flags.

By default, CLI will try to connect to a redis server running at `localhost:6379`.

```
      --config string          config file to set flag defaut values (default is $HOME/.asynq.yaml)
  -n, --db int                 redis database number (default is 0)
  -h, --help                   help for asynq
  -p, --password string        password to use when connecting to redis server
  -u, --uri string             redis server URI (default "127.0.0.1:6379")

      --cluster                connect to redis cluster
      --cluster_addrs string   list of comma-separated redis server addresses
```

## Config File

You can use a config file to set default values for the flags.

By default, `asynq` will try to read config file located in
`$HOME/.asynq.(yml|json)`. You can specify the file location via `--config` flag.

Config file example:

```yaml
uri: 127.0.0.1:6379
db: 2
password: mypassword
```

This will set the default values for `--uri`, `--db`, and `--password` flags.
