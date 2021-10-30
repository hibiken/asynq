package rate

import (
	"context"
	"flag"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/hibiken/asynq/internal/base"
	asynqcontext "github.com/hibiken/asynq/internal/context"
	"strings"
	"testing"
	"time"
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
		taskIDs        []uuid.UUID
		ctxFunc        func(uuid.UUID) (context.Context, context.CancelFunc)
		want           []bool
		wantErr        string
	}{
		{
			desc:           "Should acquire lock when current lock count is less than maxConcurrency",
			name:           "task-1",
			maxConcurrency: 3,
			taskIDs:        []uuid.UUID{uuid.New(), uuid.New()},
			ctxFunc: func(id uuid.UUID) (context.Context, context.CancelFunc) {
				return asynqcontext.New(&base.TaskMessage{
					ID:    id,
					Queue: "task-1",
				}, time.Now().Add(time.Second))
			},
			want: []bool{true, true},
		},
		{
			desc:           "Should fail acquiring lock when current lock count is equal to maxConcurrency",
			name:           "task-2",
			maxConcurrency: 3,
			taskIDs:        []uuid.UUID{uuid.New(), uuid.New(), uuid.New(), uuid.New()},
			ctxFunc: func(id uuid.UUID) (context.Context, context.CancelFunc) {
				return asynqcontext.New(&base.TaskMessage{
					ID:    id,
					Queue: "task-2",
				}, time.Now().Add(time.Second))
			},
			want: []bool{true, true, true, false},
		},
		{
			desc:           "Should return error if context has no deadline",
			name:           "task-3",
			maxConcurrency: 1,
			taskIDs:        []uuid.UUID{uuid.New(), uuid.New()},
			ctxFunc: func(id uuid.UUID) (context.Context, context.CancelFunc) {
				return context.Background(), func() {}
			},
			want:    []bool{false, false},
			wantErr: "provided context must have a deadline",
		},
		{
			desc:           "Should return error when context is missing taskID",
			name:           "task-4",
			maxConcurrency: 1,
			taskIDs:        []uuid.UUID{uuid.New()},
			ctxFunc: func(_ uuid.UUID) (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), time.Second)
			},
			want:    []bool{false},
			wantErr: "provided context is missing task ID value",
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
				if got != tt.want[i] {
					t.Errorf("%s;\nSemaphore.Acquire(ctx) returned %v, want %v", tt.desc, got, tt.want[i])
				}
				if (tt.wantErr == "" && err != nil) || (tt.wantErr != "" && (err == nil || err.Error() != tt.wantErr)) {
					t.Errorf("%s;\nSemaphore.Acquire() got error %v want error %v", tt.desc, err, tt.wantErr)
				}

				cancel()
			}
		})
	}
}

func TestNewSemaphore_Acquire_StaleLock(t *testing.T) {
	opt := getRedisConnOpt(t)
	rc := opt.MakeRedisClient().(redis.UniversalClient)
	defer rc.Close()

	taskID := uuid.New()

	rc.ZAdd(context.Background(), semaphoreKey("stale-lock"), &redis.Z{
		Score:  float64(time.Now().Add(-10 * time.Second).Unix()),
		Member: taskID.String(),
	})

	sema := NewSemaphore(opt, "stale-lock", 1)
	defer sema.Close()

	ctx, cancel := asynqcontext.New(&base.TaskMessage{
		ID:    taskID,
		Queue: "task-1",
	}, time.Now().Add(time.Second))
	defer cancel()

	got, err := sema.Acquire(ctx)
	if err != nil {
		t.Errorf("Acquire_StaleLock;\nSemaphore.Acquire() got error %v", err)
	}

	if !got {
		t.Error("Acquire_StaleLock;\nSemaphore.Acquire() got false want true")
	}
}

func TestNewSemaphore_Release(t *testing.T) {
	testID := uuid.New()

	tests := []struct {
		desc      string
		name      string
		taskIDs   []uuid.UUID
		ctxFunc   func(uuid.UUID) (context.Context, context.CancelFunc)
		wantCount int64
		wantErr   string
	}{
		{
			desc:    "Should decrease lock count",
			name:    "task-5",
			taskIDs: []uuid.UUID{uuid.New()},
			ctxFunc: func(id uuid.UUID) (context.Context, context.CancelFunc) {
				return asynqcontext.New(&base.TaskMessage{
					ID:    id,
					Queue: "task-3",
				}, time.Now().Add(time.Second))
			},
		},
		{
			desc:    "Should decrease lock count by 2",
			name:    "task-6",
			taskIDs: []uuid.UUID{uuid.New(), uuid.New()},
			ctxFunc: func(id uuid.UUID) (context.Context, context.CancelFunc) {
				return asynqcontext.New(&base.TaskMessage{
					ID:    id,
					Queue: "task-4",
				}, time.Now().Add(time.Second))
			},
		},
		{
			desc:    "Should return error when context is missing taskID",
			name:    "task-7",
			taskIDs: []uuid.UUID{uuid.New()},
			ctxFunc: func(_ uuid.UUID) (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), time.Second)
			},
			wantCount: 1,
			wantErr:   "provided context is missing task ID value",
		},
		{
			desc:    "Should return error when context has taskID which never acquired lock",
			name:    "task-8",
			taskIDs: []uuid.UUID{uuid.New()},
			ctxFunc: func(_ uuid.UUID) (context.Context, context.CancelFunc) {
				return asynqcontext.New(&base.TaskMessage{
					ID:    testID,
					Queue: "task-4",
				}, time.Now().Add(time.Second))
			},
			wantCount: 1,
			wantErr:   fmt.Sprintf("no lock found for task %q", testID.String()),
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
					Member: tt.taskIDs[i].String(),
				})
			}
			if err := rc.ZAdd(context.Background(), semaphoreKey(tt.name), members...).Err(); err != nil {
				t.Errorf("%s;\nredis.UniversalClient.ZAdd() got error %v", tt.desc, err)
			}

			sema := NewSemaphore(opt, tt.name, 3)
			defer sema.Close()

			for i := 0; i < len(tt.taskIDs); i++ {
				ctx, cancel := tt.ctxFunc(tt.taskIDs[i])

				err := sema.Release(ctx)

				if tt.wantErr == "" && err != nil {
					t.Errorf("%s;\nSemaphore.Release() got error %v", tt.desc, err)
				}

				if tt.wantErr != "" && (err == nil || err.Error() != tt.wantErr) {
					t.Errorf("%s;\nSemaphore.Release() got error %v want error %v", tt.desc, err, tt.wantErr)
				}

				cancel()
			}

			i, err := rc.ZCount(context.Background(), semaphoreKey(tt.name), "-inf", "+inf").Result()
			if err != nil {
				t.Errorf("%s;\nredis.UniversalClient.ZCount() got error %v", tt.desc, err)
			}

			if i != tt.wantCount {
				t.Errorf("%s;\nSemaphore.Release(ctx) didn't release lock, got %v want 0", tt.desc, i)
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
