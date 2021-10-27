package rate

import (
	"context"
	"flag"
	"strings"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/hibiken/asynq"
)

var (
	redisAddr string
	redisDB   int

	useRedisCluster   bool
	redisClusterAddrs string // comma-separated list of host:port
)

func init() {
	flag.StringVar(&redisAddr, "redis_addr", "localhost:6379", "redis address to use in testing")
	flag.IntVar(&redisDB, "redis_db", 14, "redis db number to use in testing")
	flag.BoolVar(&useRedisCluster, "redis_cluster", false, "use redis cluster as a broker in testing")
	flag.StringVar(&redisClusterAddrs, "redis_cluster_addrs", "localhost:7000,localhost:7001,localhost:7002", "comma separated list of redis server addresses")
}

func TestNewSemaphore(t *testing.T) {
	tests := []struct {
		desc           string
		name           string
		maxConcurrency int
		wantPanic      string
		connOpt        asynq.RedisConnOpt
	}{
		{
			desc:      "Bad RedisConnOpt",
			wantPanic: "rate.NewSemaphore: unsupported RedisConnOpt type *rate.badConnOpt",
			connOpt:   &badConnOpt{},
		},
		{
			desc:      "Zero maxConcurrency should panic",
			wantPanic: "rate.NewSemaphore: maxConcurrency cannot be less than 1",
		},
		{
			desc:           "Empty name should panic",
			maxConcurrency: 2,
			name:           "    ",
			wantPanic:      "rate.NewSemaphore: name should not be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			if tt.wantPanic != "" {
				defer func() {
					if r := recover(); r.(string) != tt.wantPanic {
						t.Errorf("%s;\nNewSemaphore should panic with msg: %s, got %s", tt.desc, tt.wantPanic, r.(string))
					}
				}()
			}

			opt := tt.connOpt
			if tt.connOpt == nil {
				opt = getRedisConnOpt(t)
			}

			sema := NewSemaphore(opt, tt.name, tt.maxConcurrency)
			defer sema.Close()
		})
	}
}

func TestNewSemaphore_Acquire(t *testing.T) {
	tests := []struct {
		desc           string
		name           string
		maxConcurrency int
		callAcquire    int
	}{
		{
			desc:           "Should acquire lock when current lock count is less than maxConcurrency",
			name:           "task-1",
			maxConcurrency: 3,
			callAcquire:    2,
		},
		{
			desc:           "Should fail acquiring lock when current lock count is equal to maxConcurrency",
			name:           "task-2",
			maxConcurrency: 3,
			callAcquire:    4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			opt := getRedisConnOpt(t)
			rc := opt.MakeRedisClient().(redis.UniversalClient)
			defer rc.Close()

			if err := rc.Del(context.Background(), semaphoreKey(tt.name)).Err(); err != nil {
				t.Errorf("%s;\nredis.UniversalClient.Del() got error %v", tt.desc, err)
			}

			sema := NewSemaphore(opt, tt.name, tt.maxConcurrency)
			defer sema.Close()

			for i := 0; i < tt.callAcquire; i++ {
				if got := sema.Acquire(context.Background()); got != (i < tt.maxConcurrency) {
					t.Errorf("%s;\nSemaphore.Acquire(ctx) returned %v, want %v", tt.desc, got, i < tt.maxConcurrency)
				}
			}
		})
	}
}

func TestNewSemaphore_Release(t *testing.T) {
	tests := []struct {
		desc           string
		name           string
		maxConcurrency int
		callRelease    int
	}{
		{
			desc:           "Should decrease lock count",
			name:           "task-3",
			maxConcurrency: 3,
			callRelease:    1,
		},
		{
			desc:           "Should decrease lock count by 2",
			name:           "task-4",
			maxConcurrency: 3,
			callRelease:    2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			opt := getRedisConnOpt(t)
			rc := opt.MakeRedisClient().(redis.UniversalClient)
			defer rc.Close()

			if err := rc.Del(context.Background(), semaphoreKey(tt.name)).Err(); err != nil {
				t.Errorf("%s;\nredis.UniversalClient.Del() got error %v", tt.desc, err)
			}

			if err := rc.IncrBy(context.Background(), semaphoreKey(tt.name), int64(tt.maxConcurrency)).Err(); err != nil {
				t.Errorf("%s;\nredis.UniversalClient.IncrBy() got error %v", tt.desc, err)
			}

			sema := NewSemaphore(opt, tt.name, tt.maxConcurrency)
			defer sema.Close()

			for i := 0; i < tt.callRelease; i++ {
				sema.Release(context.Background())
			}

			i, err := rc.Get(context.Background(), semaphoreKey(tt.name)).Int()
			if err != nil {
				t.Errorf("%s;\nredis.UniversalClient.Get() got error %v", tt.desc, err)
			}

			if i != tt.maxConcurrency-tt.callRelease {
				t.Errorf("%s;\nSemaphore.Release(ctx) didn't release lock, got %v want %v",
					tt.desc, i, tt.maxConcurrency-tt.callRelease)
			}
		})
	}
}

func getRedisConnOpt(tb testing.TB) asynq.RedisConnOpt {
	tb.Helper()
	if useRedisCluster {
		addrs := strings.Split(redisClusterAddrs, ",")
		if len(addrs) == 0 {
			tb.Fatal("No redis cluster addresses provided. Please set addresses using --redis_cluster_addrs flag.")
		}
		return asynq.RedisClusterClientOpt{
			Addrs: addrs,
		}
	}
	return asynq.RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	}
}

type badConnOpt struct {
}

func (b badConnOpt) MakeRedisClient() interface{} {
	return nil
}
