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

	"github.com/go-redis/redis/v8"
	"github.com/hibiken/asynq/internal/base"
)

// Task represents a unit of work to be performed.
type Task struct {
	// typename indicates the type of task to be performed.
	typename string

	// payload holds data needed to perform the task.
	payload []byte

	// opts holds options for the task.
	opts []Option
}

func (t *Task) Type() string    { return t.typename }
func (t *Task) Payload() []byte { return t.payload }

// NewTask returns a new Task given a type name and payload data.
// Options can be passed to configure task processing behavior.
func NewTask(typename string, payload []byte, opts ...Option) *Task {
	return &Task{
		typename: typename,
		payload:  payload,
		opts:     opts,
	}
}

// A TaskInfo describes a task and its metadata.
type TaskInfo struct {
	// ID is the identifier of the task.
	ID string

	// Queue is the name of the queue in which the task belongs.
	Queue string

	// Type is the type name of the task.
	Type string

	// Payload is the payload data of the task.
	Payload []byte

	// State indicates the task state.
	State TaskState

	// MaxRetry is the maximum number of times the task can be retried.
	MaxRetry int

	// Retried is the number of times the task has retried so far.
	Retried int

	// LastErr is the error message from the last failure.
	LastErr string

	// LastFailedAt is the time time of the last failure if any.
	// If the task has no failures, LastFailedAt is zero time (i.e. time.Time{}).
	LastFailedAt time.Time

	// Timeout is the duration the task can be processed by Handler before being retried,
	// zero if not specified
	Timeout time.Duration

	// Deadline is the deadline for the task, zero value if not specified.
	Deadline time.Time

	// NextProcessAt is the time the task is scheduled to be processed,
	// zero if not applicable.
	NextProcessAt time.Time
}

func newTaskInfo(msg *base.TaskMessage, state base.TaskState, nextProcessAt time.Time) *TaskInfo {
	info := TaskInfo{
		ID:            msg.ID.String(),
		Queue:         msg.Queue,
		Type:          msg.Type,
		Payload:       msg.Payload, // Do we need to make a copy?
		MaxRetry:      msg.Retry,
		Retried:       msg.Retried,
		LastErr:       msg.ErrorMsg,
		Timeout:       time.Duration(msg.Timeout) * time.Second,
		NextProcessAt: nextProcessAt,
	}
	if msg.LastFailedAt == 0 {
		info.LastFailedAt = time.Time{}
	} else {
		info.LastFailedAt = time.Unix(msg.LastFailedAt, 0)
	}

	if msg.Deadline == 0 {
		info.Deadline = time.Time{}
	} else {
		info.Deadline = time.Unix(msg.Deadline, 0)
	}

	switch state {
	case base.TaskStateActive:
		info.State = TaskStateActive
	case base.TaskStatePending:
		info.State = TaskStatePending
	case base.TaskStateScheduled:
		info.State = TaskStateScheduled
	case base.TaskStateRetry:
		info.State = TaskStateRetry
	case base.TaskStateArchived:
		info.State = TaskStateArchived
	default:
		panic(fmt.Sprintf("internal error: unknown state: %d", state))
	}
	return &info
}

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

func (s TaskState) String() string {
	switch s {
	case TaskStateActive:
		return "active"
	case TaskStatePending:
		return "pending"
	case TaskStateScheduled:
		return "scheduled"
	case TaskStateRetry:
		return "retry"
	case TaskStateArchived:
		return "archived"
	}
	panic("asynq: unknown task state")
}

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
