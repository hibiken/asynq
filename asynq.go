// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"crypto/tls"
	"fmt"

	"github.com/go-redis/redis/v7"
)

// Task represents a task to be performed.
type Task struct {
	// Type indicates the type of task to be performed.
	Type string

	// Payload holds data needed to process the task.
	Payload Payload
}

// NewTask returns a new instance of a task given a task type and payload.
//
// Since payload data gets serialized to JSON, the payload values must be
// composed of JSON supported data types.
func NewTask(typename string, payload map[string]interface{}) *Task {
	return &Task{
		Type:    typename,
		Payload: Payload{payload},
	}
}

// RedisConnOpt is a discriminated union of redis-client-option types.
//
// RedisConnOpt represents a sum of following types:
// RedisClientOpt | *RedisClientOpt | RedisFailoverClientOpt | *RedisFailoverClientOpt
type RedisConnOpt interface{}

// RedisClientOpt is used to specify redis client options to connect
// to a redis server running as a stand alone instance.
type RedisClientOpt struct {
	// Network type to use, either tcp or unix.
	// Default is tcp.
	Network string

	// Redis server address in the format 'host:port'.
	Addr string

	// Redis server password.
	Password string

	// Redis DB to select after connecting to the server.
	// See: https://redis.io/commands/select.
	DB int

	// Maximum number of socket connections.
	// Default is 10 connections per every CPU as reported by runtime.NumCPU.
	PoolSize int

	// TLS Config used to connect to the server.
	// TLS will be negotiated only if this field is set.
	TLSConfig *tls.Config
}

// RedisFailoverClientOpt is used to specify redis failover client.
type RedisFailoverClientOpt struct {
	// Redis master name that monitored by sentinels.
	MasterName string

	// Addresses of sentinels in the form "host:port".
	// Use at least three sentinels to avoid problems described in
	// https://redis.io/topics/sentinel.
	SentinelAddrs []string

	// Redis sentinel password.
	SentinelPassword string

	// Redis server password.
	Password string

	// Redis DB to select after connecting to the server.
	// See: https://redis.io/commands/select.
	DB int

	// Maximum number of socket connections.
	// Default is 10 connections per every CPU as reported by runtime.NumCPU.
	PoolSize int

	// TLS Config used to connect to the server.
	// TLS will be negotiated only if this field is set.
	TLSConfig *tls.Config
}

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
		panic(fmt.Sprintf("unexpected type %T for RedisConnOpt", r))
	}
}
