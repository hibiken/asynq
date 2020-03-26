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

	"github.com/go-redis/redis/v7"
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
