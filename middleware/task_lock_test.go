package middleware

import (
	"context"
	"errors"
	"flag"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/hibiken/asynq/internal/base"
	asynqcontext "github.com/hibiken/asynq/internal/context"
	h "github.com/hibiken/asynq/internal/testutil"
	"github.com/redis/go-redis/v9"
)

var (
	redisAddr string
	redisDB   int

	useRedisCluster   bool
	redisClusterAddrs string
)

func init() {
	flag.StringVar(&redisAddr, "redis_addr", "localhost:6379", "redis address to use in testing")
	flag.IntVar(&redisDB, "redis_db", 14, "redis db number to use in testing")
	flag.BoolVar(&useRedisCluster, "redis_cluster", false, "use redis cluster as a broker in testing")
	flag.StringVar(&redisClusterAddrs, "redis_cluster_addrs", "localhost:7000,localhost:7001,localhost:7002", "comma separated list of redis server addresses")
}

func setup(tb testing.TB) redis.UniversalClient {
	tb.Helper()
	if useRedisCluster {
		addrs := strings.Split(redisClusterAddrs, ",")
		if len(addrs) == 0 {
			tb.Fatal("No redis cluster addresses provided. Please set addresses using --redis_cluster_addrs flag.")
		}
		r := redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})
		h.FlushDB(tb, r)
		return r
	}
	r := redis.NewClient(&redis.Options{Addr: redisAddr, DB: redisDB})
	h.FlushDB(tb, r)
	return r
}

func getRedisConnOpt(tb testing.TB) asynq.RedisConnOpt {
	tb.Helper()
	if useRedisCluster {
		addrs := strings.Split(redisClusterAddrs, ",")
		if len(addrs) == 0 {
			tb.Fatal("No redis cluster addresses provided. Please set addresses using --redis_cluster_addrs flag.")
		}
		return asynq.RedisClusterClientOpt{Addrs: addrs}
	}
	return asynq.RedisClientOpt{Addr: redisAddr, DB: redisDB}
}

func TestTaskLockBusySkipsHandler(t *testing.T) {
	rc := setup(t)
	defer rc.Close()

	taskID := "task-1"
	lockKey := "asynq:lock:task:email:send:" + taskID
	if ok, err := rc.SetNX(context.Background(), lockKey, "other-worker", 5*time.Second).Result(); err != nil || !ok {
		t.Fatalf("rc.SetNX(%q) = (%v, %v), want (true, nil)", lockKey, ok, err)
	}

	task := asynq.NewTaskWithHeaders("email:send", nil, map[string]string{asynq.TaskLockHeader: "1"})
	ctx, cancel := asynqcontext.New(context.Background(), &base.TaskMessage{
		ID:    taskID,
		Type:  task.Type(),
		Queue: base.DefaultQueueName,
	}, time.Now().Add(30*time.Second))
	defer cancel()

	var called bool
	handler := asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error {
		called = true
		return nil
	})

	err := TaskLockFromRedisClient(rc)(handler).ProcessTask(ctx, task)
	if !errors.Is(err, asynq.RevokeTask) {
		t.Fatalf("ProcessTask error = %v, want error wrapping %v", err, asynq.RevokeTask)
	}
	if called {
		t.Fatal("handler was called while task lock was held")
	}
}

func TestTaskLockDropsDuplicateDelivery(t *testing.T) {
	opt := getRedisConnOpt(t)
	rc := setup(t)
	defer rc.Close()

	lockKey := "asynq:lock:task:email:welcome:task-1"
	if ok, err := rc.SetNX(context.Background(), lockKey, "other-worker", 5*time.Second).Result(); err != nil || !ok {
		t.Fatalf("rc.SetNX(%q) = (%v, %v), want (true, nil)", lockKey, ok, err)
	}

	var (
		mu        sync.Mutex
		processed int
	)
	done := make(chan struct{}, 1)

	mux := asynq.NewServeMux()
	mux.Use(TaskLock(opt))
	mux.HandleFunc("email:welcome", func(ctx context.Context, task *asynq.Task) error {
		mu.Lock()
		processed++
		mu.Unlock()
		select {
		case done <- struct{}{}:
		default:
		}
		return nil
	})

	srv := asynq.NewServer(opt, asynq.Config{
		Concurrency:              1,
		LogLevel:                 asynq.FatalLevel,
		DelayedTaskCheckInterval: 100 * time.Millisecond,
	})
	defer srv.Shutdown()
	if err := srv.Start(mux); err != nil {
		t.Fatalf("srv.Start() returned error: %v", err)
	}

	client := asynq.NewClient(opt)
	defer client.Close()
	inspector := asynq.NewInspector(opt)
	defer inspector.Close()

	task := asynq.NewTask("email:welcome", nil, asynq.Lock(), asynq.Retention(time.Minute), asynq.MaxRetry(5))
	if _, err := client.Enqueue(task, asynq.TaskID("task-1")); err != nil {
		t.Fatalf("client.Enqueue returned error: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		qinfo, err := inspector.GetQueueInfo(base.DefaultQueueName)
		if err == nil && qinfo.Size == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	mu.Lock()
	if processed != 0 {
		t.Fatalf("processed = %d, want 0 while lock is busy", processed)
	}
	mu.Unlock()

	select {
	case <-done:
		t.Fatal("handler should not run when duplicate delivery is locked")
	case <-time.After(300 * time.Millisecond):
	}

	qinfo, err := inspector.GetQueueInfo(base.DefaultQueueName)
	if err != nil {
		t.Fatalf("inspector.GetQueueInfo returned error: %v", err)
	}
	if qinfo.Retry != 0 {
		t.Fatalf("retry task count = %d, want 0", qinfo.Retry)
	}
	if qinfo.Archived != 0 {
		t.Fatalf("archived task count = %d, want 0", qinfo.Archived)
	}
	if qinfo.Active != 0 || qinfo.Pending != 0 {
		t.Fatalf("queue still has active=%d pending=%d, want both zero", qinfo.Active, qinfo.Pending)
	}
}

func TestTaskLockAllowsNormalProcessing(t *testing.T) {
	opt := getRedisConnOpt(t)
	rc := setup(t)
	defer rc.Close()

	done := make(chan struct{}, 1)
	mux := asynq.NewServeMux()
	mux.Use(TaskLock(opt))
	mux.HandleFunc("email:welcome", func(ctx context.Context, task *asynq.Task) error {
		select {
		case done <- struct{}{}:
		default:
		}
		return nil
	})

	srv := asynq.NewServer(opt, asynq.Config{Concurrency: 1, LogLevel: asynq.FatalLevel})
	defer srv.Shutdown()
	if err := srv.Start(mux); err != nil {
		t.Fatalf("srv.Start() returned error: %v", err)
	}

	client := asynq.NewClient(opt)
	defer client.Close()
	if _, err := client.Enqueue(asynq.NewTask("email:welcome", nil, asynq.Lock()), asynq.TaskID("task-1")); err != nil {
		t.Fatalf("client.Enqueue returned error: %v", err)
	}

	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("timed out waiting for task to be processed")
	}
}

func TestTaskLockUsesCustomKey(t *testing.T) {
	opt := getRedisConnOpt(t)
	rc := setup(t)
	defer rc.Close()

	lockKey := "asynq:lock:payment:123"
	if ok, err := rc.SetNX(context.Background(), lockKey, "other-worker", 5*time.Second).Result(); err != nil || !ok {
		t.Fatalf("rc.SetNX(%q) = (%v, %v), want (true, nil)", lockKey, ok, err)
	}

	var called bool
	mux := asynq.NewServeMux()
	mux.Use(TaskLock(opt))
	mux.HandleFunc("payments:charge", func(ctx context.Context, task *asynq.Task) error {
		called = true
		return nil
	})

	srv := asynq.NewServer(opt, asynq.Config{
		Concurrency:              1,
		LogLevel:                 asynq.FatalLevel,
		DelayedTaskCheckInterval: 100 * time.Millisecond,
	})
	defer srv.Shutdown()
	if err := srv.Start(mux); err != nil {
		t.Fatalf("srv.Start() returned error: %v", err)
	}

	client := asynq.NewClient(opt)
	defer client.Close()
	task := asynq.NewTask("payments:charge", nil, asynq.LockKey("payment:123"))
	if _, err := client.Enqueue(task, asynq.TaskID("task-1")); err != nil {
		t.Fatalf("client.Enqueue returned error: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	if called {
		t.Fatal("handler was called while custom lock key was held")
	}
}
