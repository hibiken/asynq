// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"crypto/tls"
	"fmt"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/hibiken/asynq/internal/base"
)

// Task represents a unit of work to be performed.
type Task struct {
	// Type indicates the type of task to be performed.
	Type string

	// Payload holds data needed to perform the task.
	Payload Payload
}

// NewTask returns a new Task given a type name and payload data.
//
// The payload values must be serializable.
func NewTask(typename string, payload map[string]interface{}) *Task {
	return &Task{
		Type:    typename,
		Payload: Payload{payload},
	}
}

// broker is a message broker that supports operations to manage task queues.
//
// See rdb.RDB as a reference implementation.
type broker interface {
	Enqueue(msg *base.TaskMessage) error
	EnqueueUnique(msg *base.TaskMessage, ttl time.Duration) error
	Dequeue(qnames ...string) (*base.TaskMessage, error)
	Done(msg *base.TaskMessage) error
	Requeue(msg *base.TaskMessage) error
	Schedule(msg *base.TaskMessage, processAt time.Time) error
	ScheduleUnique(msg *base.TaskMessage, processAt time.Time, ttl time.Duration) error
	Retry(msg *base.TaskMessage, processAt time.Time, errMsg string) error
	Kill(msg *base.TaskMessage, errMsg string) error
	RequeueAll() (int64, error)
	CheckAndEnqueue(qnames ...string) error
	WriteServerState(ss *base.ServerState, ttl time.Duration) error
	ClearServerState(ss *base.ServerState) error
	CancelationPubSub() (*redis.PubSub, error) // TODO: Need to decouple from redis to support other brokers
	PublishCancelation(id string) error
	Close() error
}

// RedisConnOpt is a discriminated union of types that represent Redis connection configuration option.
//
// RedisConnOpt represents a sum of following types:
//
// RedisClientOpt | *RedisClientOpt | RedisFailoverClientOpt | *RedisFailoverClientOpt
type RedisConnOpt interface{}

// RedisClientOpt is used to create a redis client that connects
// to a redis server directly.
type RedisClientOpt struct {
	// Network type to use, either tcp or unix.
	// Default is tcp.
	Network string

	// Redis server address in "host:port" format.
	Addr string

	// Redis server password.
	Password string

	// Redis DB to select after connecting to a server.
	// See: https://redis.io/commands/select.
	DB int

	// Maximum number of socket connections.
	// Default is 10 connections per every CPU as reported by runtime.NumCPU.
	PoolSize int

	// TLS Config used to connect to a server.
	// TLS will be negotiated only if this field is set.
	TLSConfig *tls.Config
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

	// Redis server password.
	Password string

	// Redis DB to select after connecting to a server.
	// See: https://redis.io/commands/select.
	DB int

	// Maximum number of socket connections.
	// Default is 10 connections per every CPU as reported by runtime.NumCPU.
	PoolSize int

	// TLS Config used to connect to a server.
	// TLS will be negotiated only if this field is set.
	TLSConfig *tls.Config
}

// createRedisClient returns a redis client given a redis connection configuration.
//
// Passing an unexpected type as a RedisConnOpt argument will cause panic.
func createRedisClient(r RedisConnOpt) *redis.Client {
	switch r := r.(type) {
	case *RedisClientOpt:
		return redis.NewClient(&redis.Options{
			Network:   r.Network,
			Addr:      r.Addr,
			Password:  r.Password,
			DB:        r.DB,
			PoolSize:  r.PoolSize,
			TLSConfig: r.TLSConfig,
		})
	case RedisClientOpt:
		return redis.NewClient(&redis.Options{
			Network:   r.Network,
			Addr:      r.Addr,
			Password:  r.Password,
			DB:        r.DB,
			PoolSize:  r.PoolSize,
			TLSConfig: r.TLSConfig,
		})
	case *RedisFailoverClientOpt:
		return redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:       r.MasterName,
			SentinelAddrs:    r.SentinelAddrs,
			SentinelPassword: r.SentinelPassword,
			Password:         r.Password,
			DB:               r.DB,
			PoolSize:         r.PoolSize,
			TLSConfig:        r.TLSConfig,
		})
	case RedisFailoverClientOpt:
		return redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:       r.MasterName,
			SentinelAddrs:    r.SentinelAddrs,
			SentinelPassword: r.SentinelPassword,
			Password:         r.Password,
			DB:               r.DB,
			PoolSize:         r.PoolSize,
			TLSConfig:        r.TLSConfig,
		})
	default:
		panic(fmt.Sprintf("asynq: unexpected type %T for RedisConnOpt", r))
	}
}
