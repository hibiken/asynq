// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/hibiken/asynq/internal/base"
)

// Task represents a unit of work to be performed.
type Task struct {
	// typename indicates the type of task to be performed.
	typename string

	// payload holds data needed to perform the task.
	payload []byte
}

func (t *Task) Type() string    { return t.typename }
func (t *Task) Payload() []byte { return t.payload }

// NewTask returns a new Task given a type name and payload data.
func NewTask(typename string, payload []byte) *Task {
	return &Task{
		typename: typename,
		payload:  payload,
	}
}

// A TaskInfo describes a task and its metadata.
type TaskInfo struct {
	msg           *base.TaskMessage
	state         base.TaskState
	nextProcessAt time.Time
}

// ID returns the id of the task.
func (info *TaskInfo) ID() string { return info.msg.ID.String() }

// Queue returns the name of the queue in which the task belongs.
func (info *TaskInfo) Queue() string { return info.msg.Queue }

// Type returns the type name of the task.
func (info *TaskInfo) Type() string { return info.msg.Type }

// Payload returns the payload data of the task.
func (info *TaskInfo) Payload() []byte { return info.msg.Payload }

func (info *TaskInfo) State() TaskState {
	switch info.state {
	case base.TaskStateActive:
		return TaskStateActive
	case base.TaskStatePending:
		return TaskStatePending
	case base.TaskStateScheduled:
		return TaskStateScheduled
	case base.TaskStateRetry:
		return TaskStateRetry
	case base.TaskStateArchived:
		return TaskStateArchived
	}
	panic("internal error: unknown state in TaskInfo")
}

// MaxRetry returns the maximum number of times the task can be retried.
func (info *TaskInfo) MaxRetry() int { return info.msg.Retry }

// Retried returns the number of times the task has retried so far.
func (info *TaskInfo) Retried() int { return info.msg.Retried }

// LastErr returns the error message from the last failure.
// If the task has no failures, returns an empty string.
func (info *TaskInfo) LastErr() string { return info.msg.ErrorMsg }

// Timeout returns the duration the task can be processed by Handler before being retried,
// zero if not specified
func (info *TaskInfo) Timeout() time.Duration {
	return time.Duration(info.msg.Timeout) * time.Second
}

// Deadline returns the deadline for the task, zero value if not specified.
func (info *TaskInfo) Deadline() time.Time {
	if info.msg.Deadline == 0 {
		return time.Time{}
	}
	return time.Unix(info.msg.Deadline, 0)
}

// NextProcessAt returns the time the task is scheduled to be processed,
// zero if not applicable.
func (info *TaskInfo) NextProcessAt() time.Time { return info.nextProcessAt }

// TaskState denotes the state of a task.
type TaskState int

const (
	// Indicates that the task is currently being processed by Handler.
	TaskStateActive TaskState = iota + 1

	// Indicates that the task is ready to be processed by Handler.
	TaskStatePending

	// Indicates that the task is scheduled to be processed some time in the future.
	TaskStateScheduled

	// Indicates that the task has previously failed and scheduled to be processed some time in the future.
	TaskStateRetry

	// Indicates that the task is archived and stored for inspection purposes.
	TaskStateArchived
)

// RedisConnOpt is a discriminated union of types that represent Redis connection configuration option.
//
// RedisConnOpt represents a sum of following types:
//
//   - RedisClientOpt
//   - RedisFailoverClientOpt
//   - RedisClusterClientOpt
type RedisConnOpt interface {
	// MakeRedisClient returns a new redis client instance.
	// Return value is intentionally opaque to hide the implementation detail of redis client.
	MakeRedisClient() interface{}
}

// RedisClientOpt is used to create a redis client that connects
// to a redis server directly.
type RedisClientOpt struct {
	// Network type to use, either tcp or unix.
	// Default is tcp.
	Network string

	// Redis server address in "host:port" format.
	Addr string

	// Username to authenticate the current connection when Redis ACLs are used.
	// See: https://redis.io/commands/auth.
	Username string

	// Password to authenticate the current connection.
	// See: https://redis.io/commands/auth.
	Password string

	// Redis DB to select after connecting to a server.
	// See: https://redis.io/commands/select.
	DB int

	// Dial timeout for establishing new connections.
	// Default is 5 seconds.
	DialTimeout time.Duration

	// Timeout for socket reads.
	// If timeout is reached, read commands will fail with a timeout error
	// instead of blocking.
	//
	// Use value -1 for no timeout and 0 for default.
	// Default is 3 seconds.
	ReadTimeout time.Duration

	// Timeout for socket writes.
	// If timeout is reached, write commands will fail with a timeout error
	// instead of blocking.
	//
	// Use value -1 for no timeout and 0 for default.
	// Default is ReadTimout.
	WriteTimeout time.Duration

	// Maximum number of socket connections.
	// Default is 10 connections per every CPU as reported by runtime.NumCPU.
	PoolSize int

	// TLS Config used to connect to a server.
	// TLS will be negotiated only if this field is set.
	TLSConfig *tls.Config
}

func (opt RedisClientOpt) MakeRedisClient() interface{} {
	return redis.NewClient(&redis.Options{
		Network:      opt.Network,
		Addr:         opt.Addr,
		Username:     opt.Username,
		Password:     opt.Password,
		DB:           opt.DB,
		DialTimeout:  opt.DialTimeout,
		ReadTimeout:  opt.ReadTimeout,
		WriteTimeout: opt.WriteTimeout,
		PoolSize:     opt.PoolSize,
		TLSConfig:    opt.TLSConfig,
	})
}

// RedisFailoverClientOpt is used to creates a redis client that talks
// to redis sentinels for service discovery and has an automatic failover
// capability.
type RedisFailoverClientOpt struct {
	// Redis master name that monitored by sentinels.
	MasterName string

	// Addresses of sentinels in "host:port" format.
	// Use at least three sentinels to avoid problems described in
	// https://redis.io/topics/sentinel.
	SentinelAddrs []string

	// Redis sentinel password.
	SentinelPassword string

	// Username to authenticate the current connection when Redis ACLs are used.
	// See: https://redis.io/commands/auth.
	Username string

	// Password to authenticate the current connection.
	// See: https://redis.io/commands/auth.
	Password string

	// Redis DB to select after connecting to a server.
	// See: https://redis.io/commands/select.
	DB int

	// Dial timeout for establishing new connections.
	// Default is 5 seconds.
	DialTimeout time.Duration

	// Timeout for socket reads.
	// If timeout is reached, read commands will fail with a timeout error
	// instead of blocking.
	//
	// Use value -1 for no timeout and 0 for default.
	// Default is 3 seconds.
	ReadTimeout time.Duration

	// Timeout for socket writes.
	// If timeout is reached, write commands will fail with a timeout error
	// instead of blocking.
	//
	// Use value -1 for no timeout and 0 for default.
	// Default is ReadTimeout
	WriteTimeout time.Duration

	// Maximum number of socket connections.
	// Default is 10 connections per every CPU as reported by runtime.NumCPU.
	PoolSize int

	// TLS Config used to connect to a server.
	// TLS will be negotiated only if this field is set.
	TLSConfig *tls.Config
}

func (opt RedisFailoverClientOpt) MakeRedisClient() interface{} {
	return redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:       opt.MasterName,
		SentinelAddrs:    opt.SentinelAddrs,
		SentinelPassword: opt.SentinelPassword,
		Username:         opt.Username,
		Password:         opt.Password,
		DB:               opt.DB,
		DialTimeout:      opt.DialTimeout,
		ReadTimeout:      opt.ReadTimeout,
		WriteTimeout:     opt.WriteTimeout,
		PoolSize:         opt.PoolSize,
		TLSConfig:        opt.TLSConfig,
	})
}

// RedisClusterClientOpt is used to creates a redis client that connects to
// redis cluster.
type RedisClusterClientOpt struct {
	// A seed list of host:port addresses of cluster nodes.
	Addrs []string

	// The maximum number of retries before giving up.
	// Command is retried on network errors and MOVED/ASK redirects.
	// Default is 8 retries.
	MaxRedirects int

	// Username to authenticate the current connection when Redis ACLs are used.
	// See: https://redis.io/commands/auth.
	Username string

	// Password to authenticate the current connection.
	// See: https://redis.io/commands/auth.
	Password string

	// Dial timeout for establishing new connections.
	// Default is 5 seconds.
	DialTimeout time.Duration

	// Timeout for socket reads.
	// If timeout is reached, read commands will fail with a timeout error
	// instead of blocking.
	//
	// Use value -1 for no timeout and 0 for default.
	// Default is 3 seconds.
	ReadTimeout time.Duration

	// Timeout for socket writes.
	// If timeout is reached, write commands will fail with a timeout error
	// instead of blocking.
	//
	// Use value -1 for no timeout and 0 for default.
	// Default is ReadTimeout.
	WriteTimeout time.Duration

	// TLS Config used to connect to a server.
	// TLS will be negotiated only if this field is set.
	TLSConfig *tls.Config
}

func (opt RedisClusterClientOpt) MakeRedisClient() interface{} {
	return redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        opt.Addrs,
		MaxRedirects: opt.MaxRedirects,
		Username:     opt.Username,
		Password:     opt.Password,
		DialTimeout:  opt.DialTimeout,
		ReadTimeout:  opt.ReadTimeout,
		WriteTimeout: opt.WriteTimeout,
		TLSConfig:    opt.TLSConfig,
	})
}

// ParseRedisURI parses redis uri string and returns RedisConnOpt if uri is valid.
// It returns a non-nil error if uri cannot be parsed.
//
// Three URI schemes are supported, which are redis:, redis-socket:, and redis-sentinel:.
// Supported formats are:
//     redis://[:password@]host[:port][/dbnumber]
//     redis-socket://[:password@]path[?db=dbnumber]
//     redis-sentinel://[:password@]host1[:port][,host2:[:port]][,hostN:[:port]][?master=masterName]
func ParseRedisURI(uri string) (RedisConnOpt, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("asynq: could not parse redis uri: %v", err)
	}
	switch u.Scheme {
	case "redis":
		return parseRedisURI(u)
	case "redis-socket":
		return parseRedisSocketURI(u)
	case "redis-sentinel":
		return parseRedisSentinelURI(u)
	default:
		return nil, fmt.Errorf("asynq: unsupported uri scheme: %q", u.Scheme)
	}
}

func parseRedisURI(u *url.URL) (RedisConnOpt, error) {
	var db int
	var err error
	if len(u.Path) > 0 {
		xs := strings.Split(strings.Trim(u.Path, "/"), "/")
		db, err = strconv.Atoi(xs[0])
		if err != nil {
			return nil, fmt.Errorf("asynq: could not parse redis uri: database number should be the first segment of the path")
		}
	}
	var password string
	if v, ok := u.User.Password(); ok {
		password = v
	}
	return RedisClientOpt{Addr: u.Host, DB: db, Password: password}, nil
}

func parseRedisSocketURI(u *url.URL) (RedisConnOpt, error) {
	const errPrefix = "asynq: could not parse redis socket uri"
	if len(u.Path) == 0 {
		return nil, fmt.Errorf("%s: path does not exist", errPrefix)
	}
	q := u.Query()
	var db int
	var err error
	if n := q.Get("db"); n != "" {
		db, err = strconv.Atoi(n)
		if err != nil {
			return nil, fmt.Errorf("%s: query param `db` should be a number", errPrefix)
		}
	}
	var password string
	if v, ok := u.User.Password(); ok {
		password = v
	}
	return RedisClientOpt{Network: "unix", Addr: u.Path, DB: db, Password: password}, nil
}

func parseRedisSentinelURI(u *url.URL) (RedisConnOpt, error) {
	addrs := strings.Split(u.Host, ",")
	master := u.Query().Get("master")
	var password string
	if v, ok := u.User.Password(); ok {
		password = v
	}
	return RedisFailoverClientOpt{MasterName: master, SentinelAddrs: addrs, Password: password}, nil
}
