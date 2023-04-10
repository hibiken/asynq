package rate

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/hibiken/asynq/internal/base"
	asynqcontext "github.com/hibiken/asynq/internal/context"
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
			desc:      "Zero maxTokens should panic",
			wantPanic: "rate.NewSemaphore: maxTokens cannot be less than 1",
		},
		{
			desc:           "Empty scope should panic",
			maxConcurrency: 2,
			name:           "    ",
			wantPanic:      "rate.NewSemaphore: scope should not be empty",
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
		taskIDs        []string
		ctxFunc        func(string) (context.Context, context.CancelFunc)
		want           []bool
	}{
		{
			desc:           "Should acquire token when current token count is less than maxTokens",
			name:           "task-1",
			maxConcurrency: 3,
			taskIDs:        []string{uuid.NewString(), uuid.NewString()},
			ctxFunc: func(id string) (context.Context, context.CancelFunc) {
				return asynqcontext.New(&base.TaskMessage{
					ID:    id,
					Queue: "task-1",
				}, time.Now().Add(time.Second))
			},
			want: []bool{true, true},
		},
		{
			desc:           "Should fail acquiring token when current token count is equal to maxTokens",
			name:           "task-2",
			maxConcurrency: 3,
			taskIDs:        []string{uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()},
			ctxFunc: func(id string) (context.Context, context.CancelFunc) {
				return asynqcontext.New(&base.TaskMessage{
					ID:    id,
					Queue: "task-2",
				}, time.Now().Add(time.Second))
			},
			want: []bool{true, true, true, false},
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

			for i := 0; i < len(tt.taskIDs); i++ {
				ctx, cancel := tt.ctxFunc(tt.taskIDs[i])

				got, err := sema.Acquire(ctx)
				if err != nil {
					t.Errorf("%s;\nSemaphore.Acquire() got error %v", tt.desc, err)
				}

				if got != tt.want[i] {
					t.Errorf("%s;\nSemaphore.Acquire(ctx) returned %v, want %v", tt.desc, got, tt.want[i])
				}

				cancel()
			}
		})
	}
}

func TestNewSemaphore_Acquire_Error(t *testing.T) {
	tests := []struct {
		desc           string
		name           string
		maxConcurrency int
		taskIDs        []string
		ctxFunc        func(string) (context.Context, context.CancelFunc)
		errStr         string
	}{
		{
			desc:           "Should return error if context has no deadline",
			name:           "task-3",
			maxConcurrency: 1,
			taskIDs:        []string{uuid.NewString(), uuid.NewString()},
			ctxFunc: func(id string) (context.Context, context.CancelFunc) {
				return context.Background(), func() {}
			},
			errStr: "provided context must have a deadline",
		},
		{
			desc:           "Should return error when context is missing taskID",
			name:           "task-4",
			maxConcurrency: 1,
			taskIDs:        []string{uuid.NewString()},
			ctxFunc: func(_ string) (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), time.Second)
			},
			errStr: "provided context is missing task ID value",
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

			for i := 0; i < len(tt.taskIDs); i++ {
				ctx, cancel := tt.ctxFunc(tt.taskIDs[i])

				_, err := sema.Acquire(ctx)
				if err == nil || err.Error() != tt.errStr {
					t.Errorf("%s;\nSemaphore.Acquire() got error %v want error %v", tt.desc, err, tt.errStr)
				}

				cancel()
			}
		})
	}
}

func TestNewSemaphore_Acquire_StaleToken(t *testing.T) {
	opt := getRedisConnOpt(t)
	rc := opt.MakeRedisClient().(redis.UniversalClient)
	defer rc.Close()

	taskID := uuid.NewString()

	// adding a set member to mimic the case where token is acquired but the goroutine crashed,
	// in which case, the token will not be explicitly removed and should be present already
	rc.ZAdd(context.Background(), semaphoreKey("stale-token"), &redis.Z{
		Score:  float64(time.Now().Add(-10 * time.Second).Unix()),
		Member: taskID,
	})

	sema := NewSemaphore(opt, "stale-token", 1)
	defer sema.Close()

	ctx, cancel := asynqcontext.New(&base.TaskMessage{
		ID:    taskID,
		Queue: "task-1",
	}, time.Now().Add(time.Second))
	defer cancel()

	got, err := sema.Acquire(ctx)
	if err != nil {
		t.Errorf("Acquire_StaleToken;\nSemaphore.Acquire() got error %v", err)
	}

	if !got {
		t.Error("Acquire_StaleToken;\nSemaphore.Acquire() got false want true")
	}
}

func TestNewSemaphore_Release(t *testing.T) {
	tests := []struct {
		desc      string
		name      string
		taskIDs   []string
		ctxFunc   func(string) (context.Context, context.CancelFunc)
		wantCount int64
	}{
		{
			desc:    "Should decrease token count",
			name:    "task-5",
			taskIDs: []string{uuid.NewString()},
			ctxFunc: func(id string) (context.Context, context.CancelFunc) {
				return asynqcontext.New(&base.TaskMessage{
					ID:    id,
					Queue: "task-3",
				}, time.Now().Add(time.Second))
			},
		},
		{
			desc:    "Should decrease token count by 2",
			name:    "task-6",
			taskIDs: []string{uuid.NewString(), uuid.NewString()},
			ctxFunc: func(id string) (context.Context, context.CancelFunc) {
				return asynqcontext.New(&base.TaskMessage{
					ID:    id,
					Queue: "task-4",
				}, time.Now().Add(time.Second))
			},
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

			var members []*redis.Z
			for i := 0; i < len(tt.taskIDs); i++ {
				members = append(members, &redis.Z{
					Score:  float64(time.Now().Add(time.Duration(i) * time.Second).Unix()),
					Member: tt.taskIDs[i],
				})
			}
			if err := rc.ZAdd(context.Background(), semaphoreKey(tt.name), members...).Err(); err != nil {
				t.Errorf("%s;\nredis.UniversalClient.ZAdd() got error %v", tt.desc, err)
			}

			sema := NewSemaphore(opt, tt.name, 3)
			defer sema.Close()

			for i := 0; i < len(tt.taskIDs); i++ {
				ctx, cancel := tt.ctxFunc(tt.taskIDs[i])

				if err := sema.Release(ctx); err != nil {
					t.Errorf("%s;\nSemaphore.Release() got error %v", tt.desc, err)
				}

				cancel()
			}

			i, err := rc.ZCount(context.Background(), semaphoreKey(tt.name), "-inf", "+inf").Result()
			if err != nil {
				t.Errorf("%s;\nredis.UniversalClient.ZCount() got error %v", tt.desc, err)
			}

			if i != tt.wantCount {
				t.Errorf("%s;\nSemaphore.Release(ctx) didn't release token, got %v want 0", tt.desc, i)
			}
		})
	}
}

func TestNewSemaphore_Release_Error(t *testing.T) {
	testID := uuid.NewString()

	tests := []struct {
		desc    string
		name    string
		taskIDs []string
		ctxFunc func(string) (context.Context, context.CancelFunc)
		errStr  string
	}{
		{
			desc:    "Should return error when context is missing taskID",
			name:    "task-7",
			taskIDs: []string{uuid.NewString()},
			ctxFunc: func(_ string) (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), time.Second)
			},
			errStr: "provided context is missing task ID value",
		},
		{
			desc:    "Should return error when context has taskID which never acquired token",
			name:    "task-8",
			taskIDs: []string{uuid.NewString()},
			ctxFunc: func(_ string) (context.Context, context.CancelFunc) {
				return asynqcontext.New(&base.TaskMessage{
					ID:    testID,
					Queue: "task-4",
				}, time.Now().Add(time.Second))
			},
			errStr: fmt.Sprintf("no token found for task %q", testID),
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

			var members []*redis.Z
			for i := 0; i < len(tt.taskIDs); i++ {
				members = append(members, &redis.Z{
					Score:  float64(time.Now().Add(time.Duration(i) * time.Second).Unix()),
					Member: tt.taskIDs[i],
				})
			}
			if err := rc.ZAdd(context.Background(), semaphoreKey(tt.name), members...).Err(); err != nil {
				t.Errorf("%s;\nredis.UniversalClient.ZAdd() got error %v", tt.desc, err)
			}

			sema := NewSemaphore(opt, tt.name, 3)
			defer sema.Close()

			for i := 0; i < len(tt.taskIDs); i++ {
				ctx, cancel := tt.ctxFunc(tt.taskIDs[i])

				if err := sema.Release(ctx); err == nil || err.Error() != tt.errStr {
					t.Errorf("%s;\nSemaphore.Release() got error %v want error %v", tt.desc, err, tt.errStr)
				}

				cancel()
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
