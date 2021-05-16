// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package inspeq

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	h "github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/log"
	"github.com/hibiken/asynq/internal/rdb"
)

// variables used for package testing.
var (
	redisAddr string
	redisDB   int

	useRedisCluster   bool
	redisClusterAddrs string // comma-separated list of host:port

	testLogLevel = asynq.FatalLevel
)

var testLogger *log.Logger

func init() {
	flag.StringVar(&redisAddr, "redis_addr", "localhost:6379", "redis address to use in testing")
	flag.IntVar(&redisDB, "redis_db", 13, "redis db number to use in testing")
	flag.BoolVar(&useRedisCluster, "redis_cluster", false, "use redis cluster as a broker in testing")
	flag.StringVar(&redisClusterAddrs, "redis_cluster_addrs", "localhost:7000,localhost:7001,localhost:7002", "comma separated list of redis server addresses")
	flag.Var(&testLogLevel, "loglevel", "log level to use in testing")

	testLogger = log.NewLogger(nil)
	testLogger.SetLevel(toInternalLogLevel(testLogLevel))
}

func toInternalLogLevel(l asynq.LogLevel) log.Level {
	switch l {
	case asynq.DebugLevel:
		return log.DebugLevel
	case asynq.InfoLevel:
		return log.InfoLevel
	case asynq.WarnLevel:
		return log.WarnLevel
	case asynq.ErrorLevel:
		return log.ErrorLevel
	case asynq.FatalLevel:
		return log.FatalLevel
	}
	panic(fmt.Sprintf("inspeq: unexpected log level: %v", l))
}

func setup(tb testing.TB) (r redis.UniversalClient) {
	tb.Helper()
	if useRedisCluster {
		addrs := strings.Split(redisClusterAddrs, ",")
		if len(addrs) == 0 {
			tb.Fatal("No redis cluster addresses provided. Please set addresses using --redis_cluster_addrs flag.")
		}
		r = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: addrs,
		})
	} else {
		r = redis.NewClient(&redis.Options{
			Addr: redisAddr,
			DB:   redisDB,
		})
	}
	// Start each test with a clean slate.
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
		return asynq.RedisClusterClientOpt{
			Addrs: addrs,
		}
	}
	return asynq.RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	}
}
func TestInspectorQueues(t *testing.T) {
	r := setup(t)
	defer r.Close()
	inspector := New(getRedisConnOpt(t))

	tests := []struct {
		queues []string
	}{
		{queues: []string{"default"}},
		{queues: []string{"custom1", "custom2"}},
		{queues: []string{"default", "custom1", "custom2"}},
		{queues: []string{}},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		for _, qname := range tc.queues {
			if err := r.SAdd(base.AllQueues, qname).Err(); err != nil {
				t.Fatalf("could not initialize all queue set: %v", err)
			}
		}
		got, err := inspector.Queues()
		if err != nil {
			t.Errorf("Queues() returned an error: %v", err)
			continue
		}
		if diff := cmp.Diff(tc.queues, got, h.SortStringSliceOpt); diff != "" {
			t.Errorf("Queues() = %v, want %v; (-want, +got)\n%s", got, tc.queues, diff)
		}
	}

}

func TestInspectorDeleteQueue(t *testing.T) {
	r := setup(t)
	defer r.Close()
	inspector := New(getRedisConnOpt(t))
	defer inspector.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")

	tests := []struct {
		pending   map[string][]*base.TaskMessage
		active    map[string][]*base.TaskMessage
		scheduled map[string][]base.Z
		retry     map[string][]base.Z
		archived  map[string][]base.Z
		qname     string // queue to remove
		force     bool
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {},
			},
			active: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {},
			},
			scheduled: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			retry: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "custom",
			force: false,
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {m3},
			},
			active: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {},
			},
			scheduled: map[string][]base.Z{
				"default": {},
				"custom":  {{Message: m4, Score: time.Now().Unix()}},
			},
			retry: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "custom",
			force: true, // allow removing non-empty queue
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllPendingQueues(t, r, tc.pending)
		h.SeedAllActiveQueues(t, r, tc.active)
		h.SeedAllScheduledQueues(t, r, tc.scheduled)
		h.SeedAllRetryQueues(t, r, tc.retry)
		h.SeedAllArchivedQueues(t, r, tc.archived)

		err := inspector.DeleteQueue(tc.qname, tc.force)
		if err != nil {
			t.Errorf("DeleteQueue(%q, %t) = %v, want nil",
				tc.qname, tc.force, err)
			continue
		}
		if r.SIsMember(base.AllQueues, tc.qname).Val() {
			t.Errorf("%q is a member of %q", tc.qname, base.AllQueues)
		}
	}
}

func TestInspectorDeleteQueueErrorQueueNotEmpty(t *testing.T) {
	r := setup(t)
	defer r.Close()
	inspector := New(getRedisConnOpt(t))
	defer inspector.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")

	tests := []struct {
		pending   map[string][]*base.TaskMessage
		active    map[string][]*base.TaskMessage
		scheduled map[string][]base.Z
		retry     map[string][]base.Z
		archived  map[string][]base.Z
		qname     string // queue to remove
		force     bool
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
			},
			active: map[string][]*base.TaskMessage{
				"default": {m3, m4},
			},
			scheduled: map[string][]base.Z{
				"default": {},
			},
			retry: map[string][]base.Z{
				"default": {},
			},
			archived: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			force: false,
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllPendingQueues(t, r, tc.pending)
		h.SeedAllActiveQueues(t, r, tc.active)
		h.SeedAllScheduledQueues(t, r, tc.scheduled)
		h.SeedAllRetryQueues(t, r, tc.retry)
		h.SeedAllArchivedQueues(t, r, tc.archived)

		err := inspector.DeleteQueue(tc.qname, tc.force)
		if !errors.Is(err, ErrQueueNotEmpty) {
			t.Errorf("DeleteQueue(%v, %t) did not return ErrQueueNotEmpty",
				tc.qname, tc.force)
		}
	}
}

func TestInspectorDeleteQueueErrorQueueNotFound(t *testing.T) {
	r := setup(t)
	defer r.Close()
	inspector := New(getRedisConnOpt(t))
	defer inspector.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")

	tests := []struct {
		pending   map[string][]*base.TaskMessage
		active    map[string][]*base.TaskMessage
		scheduled map[string][]base.Z
		retry     map[string][]base.Z
		archived  map[string][]base.Z
		qname     string // queue to remove
		force     bool
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
			},
			active: map[string][]*base.TaskMessage{
				"default": {m3, m4},
			},
			scheduled: map[string][]base.Z{
				"default": {},
			},
			retry: map[string][]base.Z{
				"default": {},
			},
			archived: map[string][]base.Z{
				"default": {},
			},
			qname: "nonexistent",
			force: false,
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllPendingQueues(t, r, tc.pending)
		h.SeedAllActiveQueues(t, r, tc.active)
		h.SeedAllScheduledQueues(t, r, tc.scheduled)
		h.SeedAllRetryQueues(t, r, tc.retry)
		h.SeedAllArchivedQueues(t, r, tc.archived)

		err := inspector.DeleteQueue(tc.qname, tc.force)
		if !errors.Is(err, ErrQueueNotFound) {
			t.Errorf("DeleteQueue(%v, %t) did not return ErrQueueNotFound",
				tc.qname, tc.force)
		}
	}
}

func TestInspectorCurrentStats(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessage("task3", nil)
	m4 := h.NewTaskMessage("task4", nil)
	m5 := h.NewTaskMessageWithQueue("task5", nil, "critical")
	m6 := h.NewTaskMessageWithQueue("task6", nil, "low")
	now := time.Now()
	timeCmpOpt := cmpopts.EquateApproxTime(time.Second)
	ignoreMemUsg := cmpopts.IgnoreFields(QueueStats{}, "MemoryUsage")

	inspector := New(getRedisConnOpt(t))

	tests := []struct {
		pending   map[string][]*base.TaskMessage
		active    map[string][]*base.TaskMessage
		scheduled map[string][]base.Z
		retry     map[string][]base.Z
		archived  map[string][]base.Z
		processed map[string]int
		failed    map[string]int
		qname     string
		want      *QueueStats
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default":  {m1},
				"critical": {m5},
				"low":      {m6},
			},
			active: map[string][]*base.TaskMessage{
				"default":  {m2},
				"critical": {},
				"low":      {},
			},
			scheduled: map[string][]base.Z{
				"default": {
					{Message: m3, Score: now.Add(time.Hour).Unix()},
					{Message: m4, Score: now.Unix()},
				},
				"critical": {},
				"low":      {},
			},
			retry: map[string][]base.Z{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			archived: map[string][]base.Z{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			processed: map[string]int{
				"default":  120,
				"critical": 100,
				"low":      42,
			},
			failed: map[string]int{
				"default":  2,
				"critical": 0,
				"low":      5,
			},
			qname: "default",
			want: &QueueStats{
				Queue:     "default",
				Size:      4,
				Pending:   1,
				Active:    1,
				Scheduled: 2,
				Retry:     0,
				Archived:  0,
				Processed: 120,
				Failed:    2,
				Paused:    false,
				Timestamp: now,
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllPendingQueues(t, r, tc.pending)
		h.SeedAllActiveQueues(t, r, tc.active)
		h.SeedAllScheduledQueues(t, r, tc.scheduled)
		h.SeedAllRetryQueues(t, r, tc.retry)
		h.SeedAllArchivedQueues(t, r, tc.archived)
		for qname, n := range tc.processed {
			processedKey := base.ProcessedKey(qname, now)
			r.Set(processedKey, n, 0)
		}
		for qname, n := range tc.failed {
			failedKey := base.FailedKey(qname, now)
			r.Set(failedKey, n, 0)
		}

		got, err := inspector.CurrentStats(tc.qname)
		if err != nil {
			t.Errorf("r.CurrentStats(%q) = %v, %v, want %v, nil",
				tc.qname, got, err, tc.want)
			continue
		}
		if diff := cmp.Diff(tc.want, got, timeCmpOpt, ignoreMemUsg); diff != "" {
			t.Errorf("r.CurrentStats(%q) = %v, %v, want %v, nil; (-want, +got)\n%s",
				tc.qname, got, err, tc.want, diff)
			continue
		}
	}

}

func TestInspectorHistory(t *testing.T) {
	r := setup(t)
	defer r.Close()
	now := time.Now().UTC()
	inspector := New(getRedisConnOpt(t))

	tests := []struct {
		qname string // queue of interest
		n     int    // number of days
	}{
		{"default", 90},
		{"custom", 7},
		{"default", 1},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)

		r.SAdd(base.AllQueues, tc.qname)
		// populate last n days data
		for i := 0; i < tc.n; i++ {
			ts := now.Add(-time.Duration(i) * 24 * time.Hour)
			processedKey := base.ProcessedKey(tc.qname, ts)
			failedKey := base.FailedKey(tc.qname, ts)
			r.Set(processedKey, (i+1)*1000, 0)
			r.Set(failedKey, (i+1)*10, 0)
		}

		got, err := inspector.History(tc.qname, tc.n)
		if err != nil {
			t.Errorf("Inspector.History(%q, %d) returned error: %v", tc.qname, tc.n, err)
			continue
		}
		if len(got) != tc.n {
			t.Errorf("Inspector.History(%q, %d) returned %d daily stats, want %d",
				tc.qname, tc.n, len(got), tc.n)
			continue
		}
		for i := 0; i < tc.n; i++ {
			want := &DailyStats{
				Queue:     tc.qname,
				Processed: (i + 1) * 1000,
				Failed:    (i + 1) * 10,
				Date:      now.Add(-time.Duration(i) * 24 * time.Hour),
			}
			// Allow 2 seconds difference in timestamp.
			timeCmpOpt := cmpopts.EquateApproxTime(2 * time.Second)
			if diff := cmp.Diff(want, got[i], timeCmpOpt); diff != "" {
				t.Errorf("Inspector.History %d days ago data; got %+v, want %+v; (-want,+got):\n%s",
					i, got[i], want, diff)
			}
		}
	}
}

func createPendingTask(msg *base.TaskMessage) *PendingTask {
	return &PendingTask{
		Task:      asynq.NewTask(msg.Type, msg.Payload),
		ID:        msg.ID.String(),
		Queue:     msg.Queue,
		MaxRetry:  msg.Retry,
		Retried:   msg.Retried,
		LastError: msg.ErrorMsg,
	}
}

func TestInspectorListPendingTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "critical")
	m4 := h.NewTaskMessageWithQueue("task4", nil, "low")

	inspector := New(getRedisConnOpt(t))

	tests := []struct {
		desc    string
		pending map[string][]*base.TaskMessage
		qname   string
		want    []*PendingTask
	}{
		{
			desc: "with default queue",
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
			},
			qname: "default",
			want: []*PendingTask{
				createPendingTask(m1),
				createPendingTask(m2),
			},
		},
		{
			desc: "with named queue",
			pending: map[string][]*base.TaskMessage{
				"default":  {m1, m2},
				"critical": {m3},
				"low":      {m4},
			},
			qname: "critical",
			want: []*PendingTask{
				createPendingTask(m3),
			},
		},
		{
			desc: "with empty queue",
			pending: map[string][]*base.TaskMessage{
				"default": {},
			},
			qname: "default",
			want:  []*PendingTask(nil),
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		for q, msgs := range tc.pending {
			h.SeedPendingQueue(t, r, msgs, q)
		}

		got, err := inspector.ListPendingTasks(tc.qname)
		if err != nil {
			t.Errorf("%s; ListPendingTasks(%q) returned error: %v",
				tc.desc, tc.qname, err)
			continue
		}
		ignoreOpt := cmpopts.IgnoreUnexported(asynq.Task{})
		if diff := cmp.Diff(tc.want, got, ignoreOpt); diff != "" {
			t.Errorf("%s; ListPendingTasks(%q) = %v, want %v; (-want,+got)\n%s",
				tc.desc, tc.qname, got, tc.want, diff)
		}
	}
}

func TestInspectorListActiveTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")

	inspector := New(getRedisConnOpt(t))

	createActiveTask := func(msg *base.TaskMessage) *ActiveTask {
		return &ActiveTask{
			Task:      asynq.NewTask(msg.Type, msg.Payload),
			ID:        msg.ID.String(),
			Queue:     msg.Queue,
			MaxRetry:  msg.Retry,
			Retried:   msg.Retried,
			LastError: msg.ErrorMsg,
		}
	}

	tests := []struct {
		desc   string
		active map[string][]*base.TaskMessage
		qname  string
		want   []*ActiveTask
	}{
		{
			desc: "with a few active tasks",
			active: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {m3, m4},
			},
			qname: "default",
			want: []*ActiveTask{
				createActiveTask(m1),
				createActiveTask(m2),
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllActiveQueues(t, r, tc.active)

		got, err := inspector.ListActiveTasks(tc.qname)
		if err != nil {
			t.Errorf("%s; ListActiveTasks(%q) returned error: %v", tc.qname, tc.desc, err)
			continue
		}
		ignoreOpt := cmpopts.IgnoreUnexported(asynq.Task{})
		if diff := cmp.Diff(tc.want, got, ignoreOpt); diff != "" {
			t.Errorf("%s; ListActiveTask(%q) = %v, want %v; (-want,+got)\n%s",
				tc.desc, tc.qname, got, tc.want, diff)
		}
	}
}

func createScheduledTask(z base.Z) *ScheduledTask {
	msg := z.Message
	return &ScheduledTask{
		Task:          asynq.NewTask(msg.Type, msg.Payload),
		ID:            msg.ID.String(),
		Queue:         msg.Queue,
		MaxRetry:      msg.Retry,
		Retried:       msg.Retried,
		LastError:     msg.ErrorMsg,
		NextProcessAt: time.Unix(z.Score, 0),
		score:         z.Score,
	}
}

func TestInspectorListScheduledTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessage("task3", nil)
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := New(getRedisConnOpt(t))

	tests := []struct {
		desc      string
		scheduled map[string][]base.Z
		qname     string
		want      []*ScheduledTask
	}{
		{
			desc: "with a few scheduled tasks",
			scheduled: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			qname: "default",
			// Should be sorted by NextProcessAt.
			want: []*ScheduledTask{
				createScheduledTask(z3),
				createScheduledTask(z1),
				createScheduledTask(z2),
			},
		},
		{
			desc: "with empty scheduled queue",
			scheduled: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  []*ScheduledTask(nil),
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllScheduledQueues(t, r, tc.scheduled)

		got, err := inspector.ListScheduledTasks(tc.qname)
		if err != nil {
			t.Errorf("%s; ListScheduledTasks(%q) returned error: %v", tc.desc, tc.qname, err)
			continue
		}
		ignoreOpt := cmpopts.IgnoreUnexported(asynq.Task{}, ScheduledTask{})
		if diff := cmp.Diff(tc.want, got, ignoreOpt); diff != "" {
			t.Errorf("%s; ListScheduledTask(%q) = %v, want %v; (-want,+got)\n%s",
				tc.desc, tc.qname, got, tc.want, diff)
		}
	}
}

func createRetryTask(z base.Z) *RetryTask {
	msg := z.Message
	return &RetryTask{
		Task:          asynq.NewTask(msg.Type, msg.Payload),
		ID:            msg.ID.String(),
		Queue:         msg.Queue,
		NextProcessAt: time.Unix(z.Score, 0),
		MaxRetry:      msg.Retry,
		Retried:       msg.Retried,
		LastError:     msg.ErrorMsg,
		score:         z.Score,
	}
}

func TestInspectorListRetryTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessage("task3", nil)
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := New(getRedisConnOpt(t))

	tests := []struct {
		desc  string
		retry map[string][]base.Z
		qname string
		want  []*RetryTask
	}{
		{
			desc: "with a few retry tasks",
			retry: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			qname: "default",
			// Should be sorted by NextProcessAt.
			want: []*RetryTask{
				createRetryTask(z3),
				createRetryTask(z1),
				createRetryTask(z2),
			},
		},
		{
			desc: "with empty retry queue",
			retry: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  []*RetryTask(nil),
		},
		// TODO(hibiken): ErrQueueNotFound when queue doesn't exist
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllRetryQueues(t, r, tc.retry)

		got, err := inspector.ListRetryTasks(tc.qname)
		if err != nil {
			t.Errorf("%s; ListRetryTasks(%q) returned error: %v", tc.desc, tc.qname, err)
			continue
		}
		ignoreOpt := cmpopts.IgnoreUnexported(asynq.Task{}, RetryTask{})
		if diff := cmp.Diff(tc.want, got, ignoreOpt); diff != "" {
			t.Errorf("%s; ListRetryTask(%q) = %v, want %v; (-want,+got)\n%s",
				tc.desc, tc.qname, got, tc.want, diff)
		}
	}
}

func createArchivedTask(z base.Z) *ArchivedTask {
	msg := z.Message
	return &ArchivedTask{
		Task:         asynq.NewTask(msg.Type, msg.Payload),
		ID:           msg.ID.String(),
		Queue:        msg.Queue,
		MaxRetry:     msg.Retry,
		Retried:      msg.Retried,
		LastFailedAt: time.Unix(z.Score, 0),
		LastError:    msg.ErrorMsg,
		score:        z.Score,
	}
}

func TestInspectorListArchivedTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessage("task3", nil)
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(-5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(-15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(-2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(-2 * time.Minute).Unix()}

	inspector := New(getRedisConnOpt(t))

	tests := []struct {
		desc     string
		archived map[string][]base.Z
		qname    string
		want     []*ArchivedTask
	}{
		{
			desc: "with a few archived tasks",
			archived: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			qname: "default",
			// Should be sorted by LastFailedAt.
			want: []*ArchivedTask{
				createArchivedTask(z2),
				createArchivedTask(z1),
				createArchivedTask(z3),
			},
		},
		{
			desc: "with empty archived queue",
			archived: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  []*ArchivedTask(nil),
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllArchivedQueues(t, r, tc.archived)

		got, err := inspector.ListArchivedTasks(tc.qname)
		if err != nil {
			t.Errorf("%s; ListArchivedTasks(%q) returned error: %v", tc.desc, tc.qname, err)
			continue
		}
		ignoreOpt := cmpopts.IgnoreUnexported(asynq.Task{}, ArchivedTask{})
		if diff := cmp.Diff(tc.want, got, ignoreOpt); diff != "" {
			t.Errorf("%s; ListArchivedTask(%q) = %v, want %v; (-want,+got)\n%s",
				tc.desc, tc.qname, got, tc.want, diff)
		}
	}
}

func TestInspectorListPagination(t *testing.T) {
	// Create 100 tasks.
	var msgs []*base.TaskMessage
	for i := 0; i <= 99; i++ {
		msgs = append(msgs,
			h.NewTaskMessage(fmt.Sprintf("task%d", i), nil))
	}
	r := setup(t)
	defer r.Close()
	h.SeedPendingQueue(t, r, msgs, base.DefaultQueueName)

	inspector := New(getRedisConnOpt(t))

	tests := []struct {
		page     int
		pageSize int
		want     []*PendingTask
	}{
		{
			page:     1,
			pageSize: 5,
			want: []*PendingTask{
				createPendingTask(msgs[0]),
				createPendingTask(msgs[1]),
				createPendingTask(msgs[2]),
				createPendingTask(msgs[3]),
				createPendingTask(msgs[4]),
			},
		},
		{
			page:     3,
			pageSize: 10,
			want: []*PendingTask{
				createPendingTask(msgs[20]),
				createPendingTask(msgs[21]),
				createPendingTask(msgs[22]),
				createPendingTask(msgs[23]),
				createPendingTask(msgs[24]),
				createPendingTask(msgs[25]),
				createPendingTask(msgs[26]),
				createPendingTask(msgs[27]),
				createPendingTask(msgs[28]),
				createPendingTask(msgs[29]),
			},
		},
	}

	for _, tc := range tests {
		got, err := inspector.ListPendingTasks("default", Page(tc.page), PageSize(tc.pageSize))
		if err != nil {
			t.Errorf("ListPendingTask('default') returned error: %v", err)
			continue
		}
		ignoreOpt := cmpopts.IgnoreUnexported(asynq.Task{})
		if diff := cmp.Diff(tc.want, got, ignoreOpt); diff != "" {
			t.Errorf("ListPendingTask('default') = %v, want %v; (-want,+got)\n%s",
				got, tc.want, diff)
		}
	}
}

func TestInspectorDeleteAllPendingTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessage("task3", nil)
	m4 := h.NewTaskMessageWithQueue("task3", nil, "custom")

	inspector := New(getRedisConnOpt(t))

	tests := []struct {
		pending     map[string][]*base.TaskMessage
		qname       string
		want        int
		wantPending map[string][]*base.TaskMessage
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2, m3},
				"custom":  {m4},
			},
			qname: "default",
			want:  3,
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {m4},
			},
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2, m3},
				"custom":  {m4},
			},
			qname: "custom",
			want:  1,
			wantPending: map[string][]*base.TaskMessage{
				"default": {m1, m2, m3},
				"custom":  {},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllPendingQueues(t, r, tc.pending)

		got, err := inspector.DeleteAllPendingTasks(tc.qname)
		if err != nil {
			t.Errorf("DeleteAllPendingTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("DeleteAllPendingTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}

		for qname, want := range tc.wantPending {
			gotPending := h.GetPendingMessages(t, r, qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("unexpected pending tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorDeleteAllScheduledTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessage("task3", nil)
	m4 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := New(getRedisConnOpt(t))

	tests := []struct {
		scheduled     map[string][]base.Z
		qname         string
		want          int
		wantScheduled map[string][]base.Z
	}{
		{
			scheduled: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			qname: "default",
			want:  3,
			wantScheduled: map[string][]base.Z{
				"default": {},
				"custom":  {z4},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  0,
			wantScheduled: map[string][]base.Z{
				"default": {},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllScheduledQueues(t, r, tc.scheduled)

		got, err := inspector.DeleteAllScheduledTasks(tc.qname)
		if err != nil {
			t.Errorf("DeleteAllScheduledTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("DeleteAllScheduledTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := h.GetScheduledEntries(t, r, qname)
			if diff := cmp.Diff(want, gotScheduled, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected scheduled tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorDeleteAllRetryTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessage("task3", nil)
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := New(getRedisConnOpt(t))

	tests := []struct {
		retry     map[string][]base.Z
		qname     string
		want      int
		wantRetry map[string][]base.Z
	}{
		{
			retry: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			qname: "default",
			want:  3,
			wantRetry: map[string][]base.Z{
				"default": {},
				"custom":  {z4},
			},
		},
		{
			retry: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  0,
			wantRetry: map[string][]base.Z{
				"default": {},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllRetryQueues(t, r, tc.retry)

		got, err := inspector.DeleteAllRetryTasks(tc.qname)
		if err != nil {
			t.Errorf("DeleteAllRetryTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("DeleteAllRetryTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantRetry {
			gotRetry := h.GetRetryEntries(t, r, qname)
			if diff := cmp.Diff(want, gotRetry, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected retry tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorDeleteAllArchivedTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessage("task3", nil)
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := New(getRedisConnOpt(t))

	tests := []struct {
		archived     map[string][]base.Z
		qname        string
		want         int
		wantArchived map[string][]base.Z
	}{
		{
			archived: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			qname: "default",
			want:  3,
			wantArchived: map[string][]base.Z{
				"default": {},
				"custom":  {z4},
			},
		},
		{
			archived: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  0,
			wantArchived: map[string][]base.Z{
				"default": {},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllArchivedQueues(t, r, tc.archived)

		got, err := inspector.DeleteAllArchivedTasks(tc.qname)
		if err != nil {
			t.Errorf("DeleteAllArchivedTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("DeleteAllArchivedTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantArchived {
			gotArchived := h.GetArchivedEntries(t, r, qname)
			if diff := cmp.Diff(want, gotArchived, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected archived tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorArchiveAllPendingTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessage("task3", nil)
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	inspector := New(getRedisConnOpt(t))

	tests := []struct {
		pending      map[string][]*base.TaskMessage
		archived     map[string][]base.Z
		qname        string
		want         int
		wantPending  map[string][]*base.TaskMessage
		wantArchived map[string][]base.Z
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2, m3},
				"custom":  {m4},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "default",
			want:  3,
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {m4},
			},
			wantArchived: map[string][]base.Z{
				"default": {
					base.Z{Message: m1, Score: now.Unix()},
					base.Z{Message: m2, Score: now.Unix()},
					base.Z{Message: m3, Score: now.Unix()},
				},
				"custom": {},
			},
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default": {},
			},
			archived: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  0,
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantArchived: map[string][]base.Z{
				"default": {},
			},
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m3},
			},
			archived: map[string][]base.Z{
				"default": {z1, z2},
			},
			qname: "default",
			want:  1,
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantArchived: map[string][]base.Z{
				"default": {
					z1,
					z2,
					base.Z{Message: m3, Score: now.Unix()},
				},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllPendingQueues(t, r, tc.pending)
		h.SeedAllArchivedQueues(t, r, tc.archived)

		got, err := inspector.ArchiveAllPendingTasks(tc.qname)
		if err != nil {
			t.Errorf("ArchiveAllPendingTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ArchiveAllPendingTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantPending {
			gotPending := h.GetPendingMessages(t, r, qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("unexpected pending tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
		for qname, want := range tc.wantArchived {
			// Allow Z.Score to differ by up to 2.
			approxOpt := cmp.Comparer(func(a, b int64) bool {
				return math.Abs(float64(a-b)) < 2
			})
			gotArchived := h.GetArchivedEntries(t, r, qname)
			if diff := cmp.Diff(want, gotArchived, h.SortZSetEntryOpt, approxOpt); diff != "" {
				t.Errorf("unexpected archived tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorArchiveAllScheduledTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessage("task3", nil)
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := New(getRedisConnOpt(t))

	tests := []struct {
		scheduled     map[string][]base.Z
		archived      map[string][]base.Z
		qname         string
		want          int
		wantScheduled map[string][]base.Z
		wantArchived  map[string][]base.Z
	}{
		{
			scheduled: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "default",
			want:  3,
			wantScheduled: map[string][]base.Z{
				"default": {},
				"custom":  {z4},
			},
			wantArchived: map[string][]base.Z{
				"default": {
					base.Z{Message: m1, Score: now.Unix()},
					base.Z{Message: m2, Score: now.Unix()},
					base.Z{Message: m3, Score: now.Unix()},
				},
				"custom": {},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {z1, z2},
			},
			archived: map[string][]base.Z{
				"default": {z3},
			},
			qname: "default",
			want:  2,
			wantScheduled: map[string][]base.Z{
				"default": {},
			},
			wantArchived: map[string][]base.Z{
				"default": {
					z3,
					base.Z{Message: m1, Score: now.Unix()},
					base.Z{Message: m2, Score: now.Unix()},
				},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {},
			},
			archived: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  0,
			wantScheduled: map[string][]base.Z{
				"default": {},
			},
			wantArchived: map[string][]base.Z{
				"default": {},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {},
			},
			archived: map[string][]base.Z{
				"default": {z1, z2},
			},
			qname: "default",
			want:  0,
			wantScheduled: map[string][]base.Z{
				"default": {},
			},
			wantArchived: map[string][]base.Z{
				"default": {z1, z2},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllScheduledQueues(t, r, tc.scheduled)
		h.SeedAllArchivedQueues(t, r, tc.archived)

		got, err := inspector.ArchiveAllScheduledTasks(tc.qname)
		if err != nil {
			t.Errorf("ArchiveAllScheduledTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ArchiveAllScheduledTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := h.GetScheduledEntries(t, r, qname)
			if diff := cmp.Diff(want, gotScheduled, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected scheduled tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
		for qname, want := range tc.wantArchived {
			// Allow Z.Score to differ by up to 2.
			approxOpt := cmp.Comparer(func(a, b int64) bool {
				return math.Abs(float64(a-b)) < 2
			})
			gotArchived := h.GetArchivedEntries(t, r, qname)
			if diff := cmp.Diff(want, gotArchived, h.SortZSetEntryOpt, approxOpt); diff != "" {
				t.Errorf("unexpected archived tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorArchiveAllRetryTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessage("task3", nil)
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := New(getRedisConnOpt(t))

	tests := []struct {
		retry        map[string][]base.Z
		archived     map[string][]base.Z
		qname        string
		want         int
		wantRetry    map[string][]base.Z
		wantArchived map[string][]base.Z
	}{
		{
			retry: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "default",
			want:  3,
			wantRetry: map[string][]base.Z{
				"default": {},
				"custom":  {z4},
			},
			wantArchived: map[string][]base.Z{
				"default": {
					base.Z{Message: m1, Score: now.Unix()},
					base.Z{Message: m2, Score: now.Unix()},
					base.Z{Message: m3, Score: now.Unix()},
				},
				"custom": {},
			},
		},
		{
			retry: map[string][]base.Z{
				"default": {z1, z2},
			},
			archived: map[string][]base.Z{
				"default": {z3},
			},
			qname: "default",
			want:  2,
			wantRetry: map[string][]base.Z{
				"default": {},
			},
			wantArchived: map[string][]base.Z{
				"default": {
					z3,
					base.Z{Message: m1, Score: now.Unix()},
					base.Z{Message: m2, Score: now.Unix()},
				},
			},
		},
		{
			retry: map[string][]base.Z{
				"default": {},
			},
			archived: map[string][]base.Z{
				"default": {z1, z2},
			},
			qname: "default",
			want:  0,
			wantRetry: map[string][]base.Z{
				"default": {},
			},
			wantArchived: map[string][]base.Z{
				"default": {z1, z2},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllRetryQueues(t, r, tc.retry)
		h.SeedAllArchivedQueues(t, r, tc.archived)

		got, err := inspector.ArchiveAllRetryTasks(tc.qname)
		if err != nil {
			t.Errorf("ArchiveAllRetryTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ArchiveAllRetryTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantRetry {
			gotRetry := h.GetRetryEntries(t, r, qname)
			if diff := cmp.Diff(want, gotRetry, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected retry tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
		cmpOpt := h.EquateInt64Approx(2) // allow for 2 seconds difference in Z.Score
		for qname, want := range tc.wantArchived {
			wantArchived := h.GetArchivedEntries(t, r, qname)
			if diff := cmp.Diff(want, wantArchived, h.SortZSetEntryOpt, cmpOpt); diff != "" {
				t.Errorf("unexpected archived tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorRunAllScheduledTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessageWithQueue("task2", nil, "critical")
	m3 := h.NewTaskMessageWithQueue("task3", nil, "low")
	m4 := h.NewTaskMessage("task4", nil)
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := New(getRedisConnOpt(t))

	tests := []struct {
		scheduled     map[string][]base.Z
		pending       map[string][]*base.TaskMessage
		qname         string
		want          int
		wantScheduled map[string][]base.Z
		wantPending   map[string][]*base.TaskMessage
	}{
		{
			scheduled: map[string][]base.Z{
				"default":  {z1, z4},
				"critical": {z2},
				"low":      {z3},
			},
			pending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			qname: "default",
			want:  2,
			wantScheduled: map[string][]base.Z{
				"default":  {},
				"critical": {z2},
				"low":      {z3},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {m1, m4},
				"critical": {},
				"low":      {},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default":  {z1},
				"critical": {z2},
				"low":      {z3},
			},
			pending: map[string][]*base.TaskMessage{
				"default":  {m4},
				"critical": {},
				"low":      {},
			},
			qname: "default",
			want:  1,
			wantScheduled: map[string][]base.Z{
				"default":  {},
				"critical": {z2},
				"low":      {z3},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {m4, m1},
				"critical": {},
				"low":      {},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {},
			},
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
			qname: "default",
			want:  0,
			wantScheduled: map[string][]base.Z{
				"default": {},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllScheduledQueues(t, r, tc.scheduled)
		h.SeedAllPendingQueues(t, r, tc.pending)

		got, err := inspector.RunAllScheduledTasks(tc.qname)
		if err != nil {
			t.Errorf("RunAllScheduledTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("RunAllScheduledTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := h.GetScheduledEntries(t, r, qname)
			if diff := cmp.Diff(want, gotScheduled, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected scheduled tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
		for qname, want := range tc.wantPending {
			gotPending := h.GetPendingMessages(t, r, qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("unexpected pending tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorRunAllRetryTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessageWithQueue("task2", nil, "critical")
	m3 := h.NewTaskMessageWithQueue("task3", nil, "low")
	m4 := h.NewTaskMessage("task2", nil)
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := New(getRedisConnOpt(t))

	tests := []struct {
		retry       map[string][]base.Z
		pending     map[string][]*base.TaskMessage
		qname       string
		want        int
		wantRetry   map[string][]base.Z
		wantPending map[string][]*base.TaskMessage
	}{
		{
			retry: map[string][]base.Z{
				"default":  {z1, z4},
				"critical": {z2},
				"low":      {z3},
			},
			pending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			qname: "default",
			want:  2,
			wantRetry: map[string][]base.Z{
				"default":  {},
				"critical": {z2},
				"low":      {z3},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {m1, m4},
				"critical": {},
				"low":      {},
			},
		},
		{
			retry: map[string][]base.Z{
				"default":  {z1},
				"critical": {z2},
				"low":      {z3},
			},
			pending: map[string][]*base.TaskMessage{
				"default":  {m4},
				"critical": {},
				"low":      {},
			},
			qname: "default",
			want:  1,
			wantRetry: map[string][]base.Z{
				"default":  {},
				"critical": {z2},
				"low":      {z3},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {m4, m1},
				"critical": {},
				"low":      {},
			},
		},
		{
			retry: map[string][]base.Z{
				"default": {},
			},
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
			qname: "default",
			want:  0,
			wantRetry: map[string][]base.Z{
				"default": {},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllRetryQueues(t, r, tc.retry)
		h.SeedAllPendingQueues(t, r, tc.pending)

		got, err := inspector.RunAllRetryTasks(tc.qname)
		if err != nil {
			t.Errorf("RunAllRetryTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("RunAllRetryTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantRetry {
			gotRetry := h.GetRetryEntries(t, r, qname)
			if diff := cmp.Diff(want, gotRetry, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected retry tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
		for qname, want := range tc.wantPending {
			gotPending := h.GetPendingMessages(t, r, qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("unexpected pending tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorRunAllArchivedTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessageWithQueue("task2", nil, "critical")
	m3 := h.NewTaskMessageWithQueue("task3", nil, "low")
	m4 := h.NewTaskMessage("task2", nil)
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(-5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(-15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(-2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(-2 * time.Minute).Unix()}

	inspector := New(getRedisConnOpt(t))

	tests := []struct {
		archived     map[string][]base.Z
		pending      map[string][]*base.TaskMessage
		qname        string
		want         int
		wantArchived map[string][]base.Z
		wantPending  map[string][]*base.TaskMessage
	}{
		{
			archived: map[string][]base.Z{
				"default":  {z1, z4},
				"critical": {z2},
				"low":      {z3},
			},
			pending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			qname: "default",
			want:  2,
			wantArchived: map[string][]base.Z{
				"default":  {},
				"critical": {z2},
				"low":      {z3},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {m1, m4},
				"critical": {},
				"low":      {},
			},
		},
		{
			archived: map[string][]base.Z{
				"default":  {z1},
				"critical": {z2},
			},
			pending: map[string][]*base.TaskMessage{
				"default":  {m4},
				"critical": {},
			},
			qname: "default",
			want:  1,
			wantArchived: map[string][]base.Z{
				"default":  {},
				"critical": {z2},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {m4, m1},
				"critical": {},
			},
		},
		{
			archived: map[string][]base.Z{
				"default": {},
			},
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
			qname: "default",
			want:  0,
			wantArchived: map[string][]base.Z{
				"default": {},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllArchivedQueues(t, r, tc.archived)
		h.SeedAllPendingQueues(t, r, tc.pending)

		got, err := inspector.RunAllArchivedTasks(tc.qname)
		if err != nil {
			t.Errorf("RunAllArchivedTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("RunAllArchivedTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantArchived {
			wantArchived := h.GetArchivedEntries(t, r, qname)
			if diff := cmp.Diff(want, wantArchived, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected archived tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}

		}
		for qname, want := range tc.wantPending {
			gotPending := h.GetPendingMessages(t, r, qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("unexpected pending tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorDeleteTaskByKeyDeletesPendingTask(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	inspector := New(getRedisConnOpt(t))

	tests := []struct {
		pending     map[string][]*base.TaskMessage
		qname       string
		key         string
		wantPending map[string][]*base.TaskMessage
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {m3},
			},
			qname: "default",
			key:   createPendingTask(m2).Key(),
			wantPending: map[string][]*base.TaskMessage{
				"default": {m1},
				"custom":  {m3},
			},
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {m3},
			},
			qname: "custom",
			key:   createPendingTask(m3).Key(),
			wantPending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllPendingQueues(t, r, tc.pending)

		if err := inspector.DeleteTaskByKey(tc.qname, tc.key); err != nil {
			t.Errorf("DeleteTaskByKey(%q, %q) returned error: %v",
				tc.qname, tc.key, err)
			continue
		}

		for qname, want := range tc.wantPending {
			got := h.GetPendingMessages(t, r, qname)
			if diff := cmp.Diff(want, got, h.SortMsgOpt); diff != "" {
				t.Errorf("unspected pending tasks in queue %q: (-want,+got):\n%s",
					qname, diff)
				continue
			}
		}
	}
}

func TestInspectorDeleteTaskByKeyDeletesScheduledTask(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := New(getRedisConnOpt(t))

	tests := []struct {
		scheduled     map[string][]base.Z
		qname         string
		key           string
		wantScheduled map[string][]base.Z
	}{
		{
			scheduled: map[string][]base.Z{
				"default": {z1, z2},
				"custom":  {z3},
			},
			qname: "default",
			key:   createScheduledTask(z2).Key(),
			wantScheduled: map[string][]base.Z{
				"default": {z1},
				"custom":  {z3},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllScheduledQueues(t, r, tc.scheduled)

		if err := inspector.DeleteTaskByKey(tc.qname, tc.key); err != nil {
			t.Errorf("DeleteTaskByKey(%q, %q) returned error: %v", tc.qname, tc.key, err)
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := h.GetScheduledEntries(t, r, qname)
			if diff := cmp.Diff(want, gotScheduled, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected scheduled tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}

		}
	}
}

func TestInspectorDeleteTaskByKeyDeletesRetryTask(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := New(getRedisConnOpt(t))

	tests := []struct {
		retry     map[string][]base.Z
		qname     string
		key       string
		wantRetry map[string][]base.Z
	}{
		{
			retry: map[string][]base.Z{
				"default": {z1, z2},
				"custom":  {z3},
			},
			qname: "default",
			key:   createRetryTask(z2).Key(),
			wantRetry: map[string][]base.Z{
				"default": {z1},
				"custom":  {z3},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllRetryQueues(t, r, tc.retry)

		if err := inspector.DeleteTaskByKey(tc.qname, tc.key); err != nil {
			t.Errorf("DeleteTaskByKey(%q, %q) returned error: %v", tc.qname, tc.key, err)
			continue
		}
		for qname, want := range tc.wantRetry {
			gotRetry := h.GetRetryEntries(t, r, qname)
			if diff := cmp.Diff(want, gotRetry, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected retry tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorDeleteTaskByKeyDeletesArchivedTask(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(-5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(-15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(-2 * time.Minute).Unix()}

	inspector := New(getRedisConnOpt(t))

	tests := []struct {
		archived     map[string][]base.Z
		qname        string
		key          string
		wantArchived map[string][]base.Z
	}{
		{
			archived: map[string][]base.Z{
				"default": {z1, z2},
				"custom":  {z3},
			},
			qname: "default",
			key:   createArchivedTask(z2).Key(),
			wantArchived: map[string][]base.Z{
				"default": {z1},
				"custom":  {z3},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllArchivedQueues(t, r, tc.archived)

		if err := inspector.DeleteTaskByKey(tc.qname, tc.key); err != nil {
			t.Errorf("DeleteTaskByKey(%q, %q) returned error: %v", tc.qname, tc.key, err)
			continue
		}
		for qname, want := range tc.wantArchived {
			wantArchived := h.GetArchivedEntries(t, r, qname)
			if diff := cmp.Diff(want, wantArchived, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected archived tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorRunTaskRunsScheduledTask(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := New(getRedisConnOpt(t))

	tests := []struct {
		scheduled     map[string][]base.Z
		pending       map[string][]*base.TaskMessage
		qname         string
		id            string
		wantScheduled map[string][]base.Z
		wantPending   map[string][]*base.TaskMessage
	}{
		{
			scheduled: map[string][]base.Z{
				"default": {z1, z2},
				"custom":  {z3},
			},
			pending: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {},
			},
			qname: "default",
			id:    createScheduledTask(z2).ID,
			wantScheduled: map[string][]base.Z{
				"default": {z1},
				"custom":  {z3},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {m2},
				"custom":  {},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllScheduledQueues(t, r, tc.scheduled)
		h.SeedAllPendingQueues(t, r, tc.pending)

		if err := inspector.RunTask(tc.qname, tc.id); err != nil {
			t.Errorf("RunTask(%q, %q) returned error: %v", tc.qname, tc.id, err)
			continue
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := h.GetScheduledEntries(t, r, qname)
			if diff := cmp.Diff(want, gotScheduled, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected scheduled tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}

		}
		for qname, want := range tc.wantPending {
			gotPending := h.GetPendingMessages(t, r, qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("unexpected pending tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
	}
}

func TestInspectorRunTaskRunsRetryTask(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessageWithQueue("task2", nil, "custom")
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := New(getRedisConnOpt(t))

	tests := []struct {
		retry       map[string][]base.Z
		pending     map[string][]*base.TaskMessage
		qname       string
		id          string
		wantRetry   map[string][]base.Z
		wantPending map[string][]*base.TaskMessage
	}{
		{
			retry: map[string][]base.Z{
				"default": {z1},
				"custom":  {z2, z3},
			},
			pending: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {},
			},
			qname: "custom",
			id:    createRetryTask(z2).ID,
			wantRetry: map[string][]base.Z{
				"default": {z1},
				"custom":  {z3},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {m2},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllRetryQueues(t, r, tc.retry)
		h.SeedAllPendingQueues(t, r, tc.pending)

		if err := inspector.RunTask(tc.qname, tc.id); err != nil {
			t.Errorf("RunTaskBy(%q, %q) returned error: %v", tc.qname, tc.id, err)
			continue
		}
		for qname, want := range tc.wantRetry {
			gotRetry := h.GetRetryEntries(t, r, qname)
			if diff := cmp.Diff(want, gotRetry, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected retry tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
		for qname, want := range tc.wantPending {
			gotPending := h.GetPendingMessages(t, r, qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("unexpected pending tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
	}
}

func TestInspectorRunTaskRunsArchivedTask(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessageWithQueue("task2", nil, "critical")
	m3 := h.NewTaskMessageWithQueue("task3", nil, "low")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(-5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(-15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(-2 * time.Minute).Unix()}

	inspector := New(getRedisConnOpt(t))

	tests := []struct {
		archived     map[string][]base.Z
		pending      map[string][]*base.TaskMessage
		qname        string
		id           string
		wantArchived map[string][]base.Z
		wantPending  map[string][]*base.TaskMessage
	}{
		{
			archived: map[string][]base.Z{
				"default":  {z1},
				"critical": {z2},
				"low":      {z3},
			},
			pending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			qname: "critical",
			id:    createArchivedTask(z2).ID,
			wantArchived: map[string][]base.Z{
				"default":  {z1},
				"critical": {},
				"low":      {z3},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {m2},
				"low":      {},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllArchivedQueues(t, r, tc.archived)
		h.SeedAllPendingQueues(t, r, tc.pending)

		if err := inspector.RunTask(tc.qname, tc.id); err != nil {
			t.Errorf("RunTask(%q, %q) returned error: %v", tc.qname, tc.id, err)
			continue
		}
		for qname, want := range tc.wantArchived {
			wantArchived := h.GetArchivedEntries(t, r, qname)
			if diff := cmp.Diff(want, wantArchived, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected archived tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
		for qname, want := range tc.wantPending {
			gotPending := h.GetPendingMessages(t, r, qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("unexpected pending tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
	}
}

func TestInspectorRunTaskError(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessageWithQueue("task2", nil, "critical")
	m3 := h.NewTaskMessageWithQueue("task3", nil, "low")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(-5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(-15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(-2 * time.Minute).Unix()}

	inspector := New(getRedisConnOpt(t))

	tests := []struct {
		archived     map[string][]base.Z
		pending      map[string][]*base.TaskMessage
		qname        string
		id           string
		wantErr      error
		wantArchived map[string][]base.Z
		wantPending  map[string][]*base.TaskMessage
	}{
		{
			archived: map[string][]base.Z{
				"default":  {z1},
				"critical": {z2},
				"low":      {z3},
			},
			pending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			qname:   "nonexistent",
			id:      createArchivedTask(z2).ID,
			wantErr: ErrQueueNotFound,
			wantArchived: map[string][]base.Z{
				"default":  {z1},
				"critical": {z2},
				"low":      {z3},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
		},
		{
			archived: map[string][]base.Z{
				"default":  {z1},
				"critical": {z2},
				"low":      {z3},
			},
			pending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			qname:   "default",
			id:      uuid.NewString(),
			wantErr: ErrTaskNotFound,
			wantArchived: map[string][]base.Z{
				"default":  {z1},
				"critical": {z2},
				"low":      {z3},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllArchivedQueues(t, r, tc.archived)
		h.SeedAllPendingQueues(t, r, tc.pending)

		if err := inspector.RunTask(tc.qname, tc.id); !errors.Is(err, tc.wantErr) {
			t.Errorf("RunTask(%q, %q) = %v, want %v", tc.qname, tc.id, err, tc.wantErr)
			continue
		}
		for qname, want := range tc.wantArchived {
			wantArchived := h.GetArchivedEntries(t, r, qname)
			if diff := cmp.Diff(want, wantArchived, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected archived tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
		for qname, want := range tc.wantPending {
			gotPending := h.GetPendingMessages(t, r, qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("unexpected pending tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
	}
}

func TestInspectorArchiveTaskByKeyArchivesPendingTask(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessageWithQueue("task2", nil, "custom")
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	inspector := New(getRedisConnOpt(t))
	now := time.Now()

	tests := []struct {
		pending      map[string][]*base.TaskMessage
		archived     map[string][]base.Z
		qname        string
		key          string
		wantPending  map[string][]*base.TaskMessage
		wantArchived map[string][]base.Z
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1},
				"custom":  {m2, m3},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "default",
			key:   createPendingTask(m1).Key(),
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {m2, m3},
			},
			wantArchived: map[string][]base.Z{
				"default": {
					{Message: m1, Score: now.Unix()},
				},
				"custom": {},
			},
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1},
				"custom":  {m2, m3},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "custom",
			key:   createPendingTask(m2).Key(),
			wantPending: map[string][]*base.TaskMessage{
				"default": {m1},
				"custom":  {m3},
			},
			wantArchived: map[string][]base.Z{
				"default": {},
				"custom": {
					{Message: m2, Score: now.Unix()},
				},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllPendingQueues(t, r, tc.pending)
		h.SeedAllArchivedQueues(t, r, tc.archived)

		if err := inspector.ArchiveTaskByKey(tc.qname, tc.key); err != nil {
			t.Errorf("ArchiveTaskByKey(%q, %q) returned error: %v",
				tc.qname, tc.key, err)
			continue
		}
		for qname, want := range tc.wantPending {
			gotPending := h.GetPendingMessages(t, r, qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("unexpected pending tasks in queue %q: (-want,+got)\n%s",
					qname, diff)
			}

		}
		for qname, want := range tc.wantArchived {
			wantArchived := h.GetArchivedEntries(t, r, qname)
			if diff := cmp.Diff(want, wantArchived, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected archived tasks in queue %q: (-want,+got)\n%s",
					qname, diff)
			}
		}
	}
}

func TestInspectorArchiveTaskByKeyArchivesScheduledTask(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessageWithQueue("task2", nil, "custom")
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := New(getRedisConnOpt(t))

	tests := []struct {
		scheduled     map[string][]base.Z
		archived      map[string][]base.Z
		qname         string
		key           string
		want          string
		wantScheduled map[string][]base.Z
		wantArchived  map[string][]base.Z
	}{
		{
			scheduled: map[string][]base.Z{
				"default": {z1},
				"custom":  {z2, z3},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "custom",
			key:   createScheduledTask(z2).Key(),
			wantScheduled: map[string][]base.Z{
				"default": {z1},
				"custom":  {z3},
			},
			wantArchived: map[string][]base.Z{
				"default": {},
				"custom": {
					{
						Message: m2,
						Score:   now.Unix(),
					},
				},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllScheduledQueues(t, r, tc.scheduled)
		h.SeedAllArchivedQueues(t, r, tc.archived)

		if err := inspector.ArchiveTaskByKey(tc.qname, tc.key); err != nil {
			t.Errorf("ArchiveTaskByKey(%q, %q) returned error: %v", tc.qname, tc.key, err)
			continue
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := h.GetScheduledEntries(t, r, qname)
			if diff := cmp.Diff(want, gotScheduled, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected scheduled tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}

		}
		for qname, want := range tc.wantArchived {
			wantArchived := h.GetArchivedEntries(t, r, qname)
			if diff := cmp.Diff(want, wantArchived, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected archived tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
	}
}

func TestInspectorArchiveTaskByKeyArchivesRetryTask(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessageWithQueue("task2", nil, "custom")
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := New(getRedisConnOpt(t))

	tests := []struct {
		retry        map[string][]base.Z
		archived     map[string][]base.Z
		qname        string
		key          string
		wantRetry    map[string][]base.Z
		wantArchived map[string][]base.Z
	}{
		{
			retry: map[string][]base.Z{
				"default": {z1},
				"custom":  {z2, z3},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "custom",
			key:   createRetryTask(z2).Key(),
			wantRetry: map[string][]base.Z{
				"default": {z1},
				"custom":  {z3},
			},
			wantArchived: map[string][]base.Z{
				"default": {},
				"custom": {
					{
						Message: m2,
						Score:   now.Unix(),
					},
				},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllRetryQueues(t, r, tc.retry)
		h.SeedAllArchivedQueues(t, r, tc.archived)

		if err := inspector.ArchiveTaskByKey(tc.qname, tc.key); err != nil {
			t.Errorf("ArchiveTaskByKey(%q, %q) returned error: %v", tc.qname, tc.key, err)
			continue
		}
		for qname, want := range tc.wantRetry {
			gotRetry := h.GetRetryEntries(t, r, qname)
			if diff := cmp.Diff(want, gotRetry, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected retry tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
		for qname, want := range tc.wantArchived {
			wantArchived := h.GetArchivedEntries(t, r, qname)
			if diff := cmp.Diff(want, wantArchived, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected archived tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
	}
}

var sortSchedulerEntry = cmp.Transformer("SortSchedulerEntry", func(in []*SchedulerEntry) []*SchedulerEntry {
	out := append([]*SchedulerEntry(nil), in...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].Spec < out[j].Spec
	})
	return out
})

func TestInspectorSchedulerEntries(t *testing.T) {
	r := setup(t)
	rdbClient := rdb.NewRDB(r)
	inspector := New(getRedisConnOpt(t))

	now := time.Now().UTC()
	schedulerID := "127.0.0.1:9876:abc123"

	tests := []struct {
		data []*base.SchedulerEntry // data to seed redis
		want []*SchedulerEntry
	}{
		{
			data: []*base.SchedulerEntry{
				{
					Spec:    "* * * * *",
					Type:    "foo",
					Payload: nil,
					Opts:    nil,
					Next:    now.Add(5 * time.Hour),
					Prev:    now.Add(-2 * time.Hour),
				},
				{
					Spec:    "@every 20m",
					Type:    "bar",
					Payload: h.JSON(map[string]interface{}{"fiz": "baz"}),
					Opts:    []string{`Queue("bar")`, `MaxRetry(20)`},
					Next:    now.Add(1 * time.Minute),
					Prev:    now.Add(-19 * time.Minute),
				},
			},
			want: []*SchedulerEntry{
				{
					Spec: "* * * * *",
					Task: asynq.NewTask("foo", nil),
					Opts: nil,
					Next: now.Add(5 * time.Hour),
					Prev: now.Add(-2 * time.Hour),
				},
				{
					Spec: "@every 20m",
					Task: asynq.NewTask("bar", h.JSON(map[string]interface{}{"fiz": "baz"})),
					Opts: []asynq.Option{asynq.Queue("bar"), asynq.MaxRetry(20)},
					Next: now.Add(1 * time.Minute),
					Prev: now.Add(-19 * time.Minute),
				},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		err := rdbClient.WriteSchedulerEntries(schedulerID, tc.data, time.Minute)
		if err != nil {
			t.Fatalf("could not write data: %v", err)
		}
		got, err := inspector.SchedulerEntries()
		if err != nil {
			t.Errorf("SchedulerEntries() returned error: %v", err)
			continue
		}
		ignoreOpt := cmpopts.IgnoreUnexported(asynq.Task{})
		if diff := cmp.Diff(tc.want, got, sortSchedulerEntry, ignoreOpt); diff != "" {
			t.Errorf("SchedulerEntries() = %v, want %v; (-want,+got)\n%s",
				got, tc.want, diff)
		}
	}
}

func TestParseOption(t *testing.T) {
	oneHourFromNow := time.Now().Add(1 * time.Hour)
	tests := []struct {
		s        string
		wantType asynq.OptionType
		wantVal  interface{}
	}{
		{`MaxRetry(10)`, asynq.MaxRetryOpt, 10},
		{`Queue("email")`, asynq.QueueOpt, "email"},
		{`Timeout(3m)`, asynq.TimeoutOpt, 3 * time.Minute},
		{asynq.Deadline(oneHourFromNow).String(), asynq.DeadlineOpt, oneHourFromNow},
		{`Unique(1h)`, asynq.UniqueOpt, 1 * time.Hour},
		{asynq.ProcessAt(oneHourFromNow).String(), asynq.ProcessAtOpt, oneHourFromNow},
		{`ProcessIn(10m)`, asynq.ProcessInOpt, 10 * time.Minute},
	}

	for _, tc := range tests {
		t.Run(tc.s, func(t *testing.T) {
			got, err := parseOption(tc.s)
			if err != nil {
				t.Fatalf("returned error: %v", err)
			}
			if got == nil {
				t.Fatal("returned nil")
			}
			if got.Type() != tc.wantType {
				t.Fatalf("got type %v, want type %v ", got.Type(), tc.wantType)
			}
			switch tc.wantType {
			case asynq.QueueOpt:
				gotVal, ok := got.Value().(string)
				if !ok {
					t.Fatal("returned Option with non-string value")
				}
				if gotVal != tc.wantVal.(string) {
					t.Fatalf("got value %v, want %v", gotVal, tc.wantVal)
				}
			case asynq.MaxRetryOpt:
				gotVal, ok := got.Value().(int)
				if !ok {
					t.Fatal("returned Option with non-int value")
				}
				if gotVal != tc.wantVal.(int) {
					t.Fatalf("got value %v, want %v", gotVal, tc.wantVal)
				}
			case asynq.TimeoutOpt, asynq.UniqueOpt, asynq.ProcessInOpt:
				gotVal, ok := got.Value().(time.Duration)
				if !ok {
					t.Fatal("returned Option with non duration value")
				}
				if gotVal != tc.wantVal.(time.Duration) {
					t.Fatalf("got value %v, want %v", gotVal, tc.wantVal)
				}
			case asynq.DeadlineOpt, asynq.ProcessAtOpt:
				gotVal, ok := got.Value().(time.Time)
				if !ok {
					t.Fatal("returned Option with non time value")
				}
				if cmp.Equal(gotVal, tc.wantVal.(time.Time)) {
					t.Fatalf("got value %v, want %v", gotVal, tc.wantVal)
				}
			default:
				t.Fatalf("returned Option with unexpected type: %v", got.Type())
			}
		})
	}
}
