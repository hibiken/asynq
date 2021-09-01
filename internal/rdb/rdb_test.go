// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"encoding/json"
	"flag"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	h "github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
)

// variables used for package testing.
var (
	redisAddr string
	redisDB   int

	useRedisCluster   bool
	redisClusterAddrs string // comma-separated list of host:port
)

func init() {
	flag.StringVar(&redisAddr, "redis_addr", "localhost:6379", "redis address to use in testing")
	flag.IntVar(&redisDB, "redis_db", 15, "redis db number to use in testing")
	flag.BoolVar(&useRedisCluster, "redis_cluster", false, "use redis cluster as a broker in testing")
	flag.StringVar(&redisClusterAddrs, "redis_cluster_addrs", "localhost:7000,localhost:7001,localhost:7002", "comma separated list of redis server addresses")
}

func setup(tb testing.TB) (r *RDB) {
	tb.Helper()
	if useRedisCluster {
		addrs := strings.Split(redisClusterAddrs, ",")
		if len(addrs) == 0 {
			tb.Fatal("No redis cluster addresses provided. Please set addresses using --redis_cluster_addrs flag.")
		}
		r = NewRDB(redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: addrs,
		}))
	} else {
		r = NewRDB(redis.NewClient(&redis.Options{
			Addr: redisAddr,
			DB:   redisDB,
		}))
	}
	// Start each test with a clean slate.
	h.FlushDB(tb, r.client)
	return r
}

func TestEnqueue(t *testing.T) {
	r := setup(t)
	defer r.Close()
	t1 := h.NewTaskMessage("send_email", h.JSON(map[string]interface{}{"to": "exampleuser@gmail.com", "from": "noreply@example.com"}))
	t2 := h.NewTaskMessageWithQueue("generate_csv", h.JSON(map[string]interface{}{}), "csv")
	t3 := h.NewTaskMessageWithQueue("sync", nil, "low")

	tests := []struct {
		msg *base.TaskMessage
	}{
		{t1},
		{t2},
		{t3},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case.

		err := r.Enqueue(tc.msg)
		if err != nil {
			t.Errorf("(*RDB).Enqueue(msg) = %v, want nil", err)
			continue
		}

		// Check Pending list has task ID.
		pendingKey := base.PendingKey(tc.msg.Queue)
		pendingIDs := r.client.LRange(pendingKey, 0, -1).Val()
		if n := len(pendingIDs); n != 1 {
			t.Errorf("Redis LIST %q contains %d IDs, want 1", pendingKey, n)
			continue
		}
		if pendingIDs[0] != tc.msg.ID.String() {
			t.Errorf("Redis LIST %q: got %v, want %v", pendingKey, pendingIDs, []string{tc.msg.ID.String()})
			continue
		}

		// Check the value under the task key.
		taskKey := base.TaskKey(tc.msg.Queue, tc.msg.ID.String())
		encoded := r.client.HGet(taskKey, "msg").Val() // "msg" field
		decoded := h.MustUnmarshal(t, encoded)
		if diff := cmp.Diff(tc.msg, decoded); diff != "" {
			t.Errorf("persisted message was %v, want %v; (-want, +got)\n%s", decoded, tc.msg, diff)
		}
		state := r.client.HGet(taskKey, "state").Val() // "state" field
		if state != "pending" {
			t.Errorf("state field under task-key is set to %q, want %q", state, "pending")
		}
		timeout := r.client.HGet(taskKey, "timeout").Val() // "timeout" field
		if want := strconv.Itoa(int(tc.msg.Timeout)); timeout != want {
			t.Errorf("timeout field under task-key is set to %v, want %v", timeout, want)
		}
		deadline := r.client.HGet(taskKey, "deadline").Val() // "deadline" field
		if want := strconv.Itoa(int(tc.msg.Deadline)); deadline != want {
			t.Errorf("deadline field under task-key is set to %v, want %v", deadline, want)
		}

		// Check queue is in the AllQueues set.
		if !r.client.SIsMember(base.AllQueues, tc.msg.Queue).Val() {
			t.Errorf("%q is not a member of SET %q", tc.msg.Queue, base.AllQueues)
		}
	}
}

func TestEnqueueUnique(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := base.TaskMessage{
		ID:        uuid.New(),
		Type:      "email",
		Payload:   h.JSON(map[string]interface{}{"user_id": json.Number("123")}),
		Queue:     base.DefaultQueueName,
		UniqueKey: base.UniqueKey(base.DefaultQueueName, "email", h.JSON(map[string]interface{}{"user_id": 123})),
	}

	tests := []struct {
		msg *base.TaskMessage
		ttl time.Duration // uniqueness ttl
	}{
		{&m1, time.Minute},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case.

		// Enqueue the first message, should succeed.
		err := r.EnqueueUnique(tc.msg, tc.ttl)
		if err != nil {
			t.Errorf("First message: (*RDB).EnqueueUnique(%v, %v) = %v, want nil",
				tc.msg, tc.ttl, err)
			continue
		}
		gotPending := h.GetPendingMessages(t, r.client, tc.msg.Queue)
		if len(gotPending) != 1 {
			t.Errorf("%q has length %d, want 1", base.PendingKey(tc.msg.Queue), len(gotPending))
			continue
		}
		if diff := cmp.Diff(tc.msg, gotPending[0]); diff != "" {
			t.Errorf("persisted data differed from the original input (-want, +got)\n%s", diff)
		}
		if !r.client.SIsMember(base.AllQueues, tc.msg.Queue).Val() {
			t.Errorf("%q is not a member of SET %q", tc.msg.Queue, base.AllQueues)
		}

		// Check Pending list has task ID.
		pendingKey := base.PendingKey(tc.msg.Queue)
		pendingIDs := r.client.LRange(pendingKey, 0, -1).Val()
		if len(pendingIDs) != 1 {
			t.Errorf("Redis LIST %q contains %d IDs, want 1", pendingKey, len(pendingIDs))
			continue
		}
		if pendingIDs[0] != tc.msg.ID.String() {
			t.Errorf("Redis LIST %q: got %v, want %v", pendingKey, pendingIDs, []string{tc.msg.ID.String()})
			continue
		}

		// Check the value under the task key.
		taskKey := base.TaskKey(tc.msg.Queue, tc.msg.ID.String())
		encoded := r.client.HGet(taskKey, "msg").Val() // "msg" field
		decoded := h.MustUnmarshal(t, encoded)
		if diff := cmp.Diff(tc.msg, decoded); diff != "" {
			t.Errorf("persisted message was %v, want %v; (-want, +got)\n%s", decoded, tc.msg, diff)
		}
		state := r.client.HGet(taskKey, "state").Val() // "state" field
		if state != "pending" {
			t.Errorf("state field under task-key is set to %q, want %q", state, "pending")
		}
		timeout := r.client.HGet(taskKey, "timeout").Val() // "timeout" field
		if want := strconv.Itoa(int(tc.msg.Timeout)); timeout != want {
			t.Errorf("timeout field under task-key is set to %v, want %v", timeout, want)
		}
		deadline := r.client.HGet(taskKey, "deadline").Val() // "deadline" field
		if want := strconv.Itoa(int(tc.msg.Deadline)); deadline != want {
			t.Errorf("deadline field under task-key is set to %v, want %v", deadline, want)
		}
		uniqueKey := r.client.HGet(taskKey, "unique_key").Val() // "unique_key" field
		if uniqueKey != tc.msg.UniqueKey {
			t.Errorf("uniqueue_key field under task key is set to %q, want %q", uniqueKey, tc.msg.UniqueKey)
		}

		// Check queue is in the AllQueues set.
		if !r.client.SIsMember(base.AllQueues, tc.msg.Queue).Val() {
			t.Errorf("%q is not a member of SET %q", tc.msg.Queue, base.AllQueues)
		}

		// Enqueue the second message, should fail.
		got := r.EnqueueUnique(tc.msg, tc.ttl)
		if !errors.Is(got, errors.ErrDuplicateTask) {
			t.Errorf("Second message: (*RDB).EnqueueUnique(msg, ttl) = %v, want %v", got, errors.ErrDuplicateTask)
			continue
		}
		gotTTL := r.client.TTL(tc.msg.UniqueKey).Val()
		if !cmp.Equal(tc.ttl.Seconds(), gotTTL.Seconds(), cmpopts.EquateApprox(0, 2)) {
			t.Errorf("TTL %q = %v, want %v", tc.msg.UniqueKey, gotTTL, tc.ttl)
			continue
		}
	}
}

func TestDequeue(t *testing.T) {
	r := setup(t)
	defer r.Close()
	now := time.Now()
	t1 := &base.TaskMessage{
		ID:       uuid.New(),
		Type:     "send_email",
		Payload:  h.JSON(map[string]interface{}{"subject": "hello!"}),
		Queue:    "default",
		Timeout:  1800,
		Deadline: 0,
	}
	t1Deadline := now.Unix() + t1.Timeout
	t2 := &base.TaskMessage{
		ID:       uuid.New(),
		Type:     "export_csv",
		Payload:  nil,
		Queue:    "critical",
		Timeout:  0,
		Deadline: 1593021600,
	}
	t2Deadline := t2.Deadline
	t3 := &base.TaskMessage{
		ID:       uuid.New(),
		Type:     "reindex",
		Payload:  nil,
		Queue:    "low",
		Timeout:  int64((5 * time.Minute).Seconds()),
		Deadline: time.Now().Add(10 * time.Minute).Unix(),
	}

	tests := []struct {
		pending       map[string][]*base.TaskMessage
		args          []string // list of queues to query
		wantMsg       *base.TaskMessage
		wantDeadline  time.Time
		wantPending   map[string][]*base.TaskMessage
		wantActive    map[string][]*base.TaskMessage
		wantDeadlines map[string][]base.Z
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {t1},
			},
			args:         []string{"default"},
			wantMsg:      t1,
			wantDeadline: time.Unix(t1Deadline, 0),
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantActive: map[string][]*base.TaskMessage{
				"default": {t1},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}},
			},
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {t2},
				"low":      {t3},
			},
			args:         []string{"critical", "default", "low"},
			wantMsg:      t2,
			wantDeadline: time.Unix(t2Deadline, 0),
			wantPending: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {},
				"low":      {t3},
			},
			wantActive: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {t2},
				"low":      {},
			},
			wantDeadlines: map[string][]base.Z{
				"default":  {},
				"critical": {{Message: t2, Score: t2Deadline}},
				"low":      {},
			},
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {},
				"low":      {t3},
			},
			args:         []string{"critical", "default", "low"},
			wantMsg:      t1,
			wantDeadline: time.Unix(t1Deadline, 0),
			wantPending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {t3},
			},
			wantActive: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {},
				"low":      {},
			},
			wantDeadlines: map[string][]base.Z{
				"default":  {{Message: t1, Score: t1Deadline}},
				"critical": {},
				"low":      {},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedAllPendingQueues(t, r.client, tc.pending)

		gotMsg, gotDeadline, err := r.Dequeue(tc.args...)
		if err != nil {
			t.Errorf("(*RDB).Dequeue(%v) returned error %v", tc.args, err)
			continue
		}
		if !cmp.Equal(gotMsg, tc.wantMsg) {
			t.Errorf("(*RDB).Dequeue(%v) returned message %v; want %v",
				tc.args, gotMsg, tc.wantMsg)
			continue
		}
		if !cmp.Equal(gotDeadline, tc.wantDeadline, cmpopts.EquateApproxTime(1*time.Second)) {
			t.Errorf("(*RDB).Dequeue(%v) returned deadline %v; want %v",
				tc.args, gotDeadline, tc.wantDeadline)
			continue
		}

		for queue, want := range tc.wantPending {
			gotPending := h.GetPendingMessages(t, r.client, queue)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want,+got):\n%s", base.PendingKey(queue), diff)
			}
		}
		for queue, want := range tc.wantActive {
			gotActive := h.GetActiveMessages(t, r.client, queue)
			if diff := cmp.Diff(want, gotActive, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want,+got):\n%s", base.ActiveKey(queue), diff)
			}
		}
		for queue, want := range tc.wantDeadlines {
			gotDeadlines := h.GetDeadlinesEntries(t, r.client, queue)
			if diff := cmp.Diff(want, gotDeadlines, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want,+got):\n%s", base.DeadlinesKey(queue), diff)
			}
		}
	}
}

func TestDequeueError(t *testing.T) {
	r := setup(t)
	defer r.Close()

	tests := []struct {
		pending       map[string][]*base.TaskMessage
		args          []string // list of queues to query
		wantErr       error
		wantPending   map[string][]*base.TaskMessage
		wantActive    map[string][]*base.TaskMessage
		wantDeadlines map[string][]base.Z
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {},
			},
			args:    []string{"default"},
			wantErr: errors.ErrNoProcessableTask,
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantActive: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {},
			},
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			args:    []string{"critical", "default", "low"},
			wantErr: errors.ErrNoProcessableTask,
			wantPending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			wantActive: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			wantDeadlines: map[string][]base.Z{
				"default":  {},
				"critical": {},
				"low":      {},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedAllPendingQueues(t, r.client, tc.pending)

		gotMsg, gotDeadline, gotErr := r.Dequeue(tc.args...)
		if !errors.Is(gotErr, tc.wantErr) {
			t.Errorf("(*RDB).Dequeue(%v) returned error %v; want %v",
				tc.args, gotErr, tc.wantErr)
			continue
		}
		if gotMsg != nil {
			t.Errorf("(*RDB).Dequeue(%v) returned message %v; want nil", tc.args, gotMsg)
			continue
		}
		if !gotDeadline.IsZero() {
			t.Errorf("(*RDB).Dequeue(%v) returned deadline %v; want %v", tc.args, gotDeadline, time.Time{})
			continue
		}

		for queue, want := range tc.wantPending {
			gotPending := h.GetPendingMessages(t, r.client, queue)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want,+got):\n%s", base.PendingKey(queue), diff)
			}
		}
		for queue, want := range tc.wantActive {
			gotActive := h.GetActiveMessages(t, r.client, queue)
			if diff := cmp.Diff(want, gotActive, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want,+got):\n%s", base.ActiveKey(queue), diff)
			}
		}
		for queue, want := range tc.wantDeadlines {
			gotDeadlines := h.GetDeadlinesEntries(t, r.client, queue)
			if diff := cmp.Diff(want, gotDeadlines, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want,+got):\n%s", base.DeadlinesKey(queue), diff)
			}
		}
	}
}

func TestDequeueIgnoresPausedQueues(t *testing.T) {
	r := setup(t)
	defer r.Close()
	t1 := &base.TaskMessage{
		ID:       uuid.New(),
		Type:     "send_email",
		Payload:  h.JSON(map[string]interface{}{"subject": "hello!"}),
		Queue:    "default",
		Timeout:  1800,
		Deadline: 0,
	}
	t2 := &base.TaskMessage{
		ID:       uuid.New(),
		Type:     "export_csv",
		Payload:  nil,
		Queue:    "critical",
		Timeout:  1800,
		Deadline: 0,
	}

	tests := []struct {
		paused      []string // list of paused queues
		pending     map[string][]*base.TaskMessage
		args        []string // list of queues to query
		wantMsg     *base.TaskMessage
		wantErr     error
		wantPending map[string][]*base.TaskMessage
		wantActive  map[string][]*base.TaskMessage
	}{
		{
			paused: []string{"default"},
			pending: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {t2},
			},
			args:    []string{"default", "critical"},
			wantMsg: t2,
			wantErr: nil,
			wantPending: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {},
			},
			wantActive: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {t2},
			},
		},
		{
			paused: []string{"default"},
			pending: map[string][]*base.TaskMessage{
				"default": {t1},
			},
			args:    []string{"default"},
			wantMsg: nil,
			wantErr: errors.ErrNoProcessableTask,
			wantPending: map[string][]*base.TaskMessage{
				"default": {t1},
			},
			wantActive: map[string][]*base.TaskMessage{
				"default": {},
			},
		},
		{
			paused: []string{"critical", "default"},
			pending: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {t2},
			},
			args:    []string{"default", "critical"},
			wantMsg: nil,
			wantErr: errors.ErrNoProcessableTask,
			wantPending: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {t2},
			},
			wantActive: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		for _, qname := range tc.paused {
			if err := r.Pause(qname); err != nil {
				t.Fatal(err)
			}
		}
		h.SeedAllPendingQueues(t, r.client, tc.pending)

		got, _, err := r.Dequeue(tc.args...)
		if !cmp.Equal(got, tc.wantMsg) || !errors.Is(err, tc.wantErr) {
			t.Errorf("Dequeue(%v) = %v, %v; want %v, %v",
				tc.args, got, err, tc.wantMsg, tc.wantErr)
			continue
		}

		for queue, want := range tc.wantPending {
			gotPending := h.GetPendingMessages(t, r.client, queue)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want,+got):\n%s", base.PendingKey(queue), diff)
			}
		}
		for queue, want := range tc.wantActive {
			gotActive := h.GetActiveMessages(t, r.client, queue)
			if diff := cmp.Diff(want, gotActive, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want,+got):\n%s", base.ActiveKey(queue), diff)
			}
		}
	}
}

func TestDone(t *testing.T) {
	r := setup(t)
	defer r.Close()
	now := time.Now()
	t1 := &base.TaskMessage{
		ID:       uuid.New(),
		Type:     "send_email",
		Payload:  nil,
		Timeout:  1800,
		Deadline: 0,
		Queue:    "default",
	}
	t2 := &base.TaskMessage{
		ID:       uuid.New(),
		Type:     "export_csv",
		Payload:  nil,
		Timeout:  0,
		Deadline: 1592485787,
		Queue:    "custom",
	}
	t3 := &base.TaskMessage{
		ID:        uuid.New(),
		Type:      "reindex",
		Payload:   nil,
		Timeout:   1800,
		Deadline:  0,
		UniqueKey: "asynq:{default}:unique:reindex:nil",
		Queue:     "default",
	}
	t1Deadline := now.Unix() + t1.Timeout
	t2Deadline := t2.Deadline
	t3Deadline := now.Unix() + t3.Deadline

	tests := []struct {
		desc          string
		active        map[string][]*base.TaskMessage // initial state of the active list
		deadlines     map[string][]base.Z            // initial state of deadlines set
		target        *base.TaskMessage              // task to remove
		wantActive    map[string][]*base.TaskMessage // final state of the active list
		wantDeadlines map[string][]base.Z            // final state of the deadline set
	}{
		{
			desc: "removes message from the correct queue",
			active: map[string][]*base.TaskMessage{
				"default": {t1},
				"custom":  {t2},
			},
			deadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}},
				"custom":  {{Message: t2, Score: t2Deadline}},
			},
			target: t1,
			wantActive: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {t2},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {},
				"custom":  {{Message: t2, Score: t2Deadline}},
			},
		},
		{
			desc: "with one queue",
			active: map[string][]*base.TaskMessage{
				"default": {t1},
			},
			deadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}},
			},
			target: t1,
			wantActive: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {},
			},
		},
		{
			desc: "with multiple messages in a queue",
			active: map[string][]*base.TaskMessage{
				"default": {t1, t3},
				"custom":  {t2},
			},
			deadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}, {Message: t3, Score: t3Deadline}},
				"custom":  {{Message: t2, Score: t2Deadline}},
			},
			target: t3,
			wantActive: map[string][]*base.TaskMessage{
				"default": {t1},
				"custom":  {t2},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}},
				"custom":  {{Message: t2, Score: t2Deadline}},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedAllDeadlines(t, r.client, tc.deadlines)
		h.SeedAllActiveQueues(t, r.client, tc.active)
		for _, msgs := range tc.active {
			for _, msg := range msgs {
				// Set uniqueness lock if unique key is present.
				if len(msg.UniqueKey) > 0 {
					err := r.client.SetNX(msg.UniqueKey, msg.ID.String(), time.Minute).Err()
					if err != nil {
						t.Fatal(err)
					}
				}
			}
		}

		err := r.Done(tc.target)
		if err != nil {
			t.Errorf("%s; (*RDB).Done(task) = %v, want nil", tc.desc, err)
			continue
		}

		for queue, want := range tc.wantActive {
			gotActive := h.GetActiveMessages(t, r.client, queue)
			if diff := cmp.Diff(want, gotActive, h.SortMsgOpt); diff != "" {
				t.Errorf("%s; mismatch found in %q: (-want, +got):\n%s", tc.desc, base.ActiveKey(queue), diff)
				continue
			}
		}
		for queue, want := range tc.wantDeadlines {
			gotDeadlines := h.GetDeadlinesEntries(t, r.client, queue)
			if diff := cmp.Diff(want, gotDeadlines, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("%s; mismatch found in %q: (-want, +got):\n%s", tc.desc, base.DeadlinesKey(queue), diff)
				continue
			}
		}

		processedKey := base.ProcessedKey(tc.target.Queue, time.Now())
		gotProcessed := r.client.Get(processedKey).Val()
		if gotProcessed != "1" {
			t.Errorf("%s; GET %q = %q, want 1", tc.desc, processedKey, gotProcessed)
		}

		gotTTL := r.client.TTL(processedKey).Val()
		if gotTTL > statsTTL {
			t.Errorf("%s; TTL %q = %v, want less than or equal to %v", tc.desc, processedKey, gotTTL, statsTTL)
		}

		if len(tc.target.UniqueKey) > 0 && r.client.Exists(tc.target.UniqueKey).Val() != 0 {
			t.Errorf("%s; Uniqueness lock %q still exists", tc.desc, tc.target.UniqueKey)
		}
	}
}

func TestRequeue(t *testing.T) {
	r := setup(t)
	defer r.Close()
	now := time.Now()
	t1 := &base.TaskMessage{
		ID:      uuid.New(),
		Type:    "send_email",
		Payload: nil,
		Queue:   "default",
		Timeout: 1800,
	}
	t2 := &base.TaskMessage{
		ID:      uuid.New(),
		Type:    "export_csv",
		Payload: nil,
		Queue:   "default",
		Timeout: 3000,
	}
	t3 := &base.TaskMessage{
		ID:      uuid.New(),
		Type:    "send_email",
		Payload: nil,
		Queue:   "critical",
		Timeout: 80,
	}
	t1Deadline := now.Unix() + t1.Timeout
	t2Deadline := now.Unix() + t2.Timeout
	t3Deadline := now.Unix() + t3.Timeout

	tests := []struct {
		pending       map[string][]*base.TaskMessage // initial state of queues
		active        map[string][]*base.TaskMessage // initial state of the active list
		deadlines     map[string][]base.Z            // initial state of the deadlines set
		target        *base.TaskMessage              // task to requeue
		wantPending   map[string][]*base.TaskMessage // final state of queues
		wantActive    map[string][]*base.TaskMessage // final state of the active list
		wantDeadlines map[string][]base.Z            // final state of the deadlines set
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {},
			},
			active: map[string][]*base.TaskMessage{
				"default": {t1, t2},
			},
			deadlines: map[string][]base.Z{
				"default": {
					{Message: t1, Score: t1Deadline},
					{Message: t2, Score: t2Deadline},
				},
			},
			target: t1,
			wantPending: map[string][]*base.TaskMessage{
				"default": {t1},
			},
			wantActive: map[string][]*base.TaskMessage{
				"default": {t2},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {
					{Message: t2, Score: t2Deadline},
				},
			},
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default": {t1},
			},
			active: map[string][]*base.TaskMessage{
				"default": {t2},
			},
			deadlines: map[string][]base.Z{
				"default": {
					{Message: t2, Score: t2Deadline},
				},
			},
			target: t2,
			wantPending: map[string][]*base.TaskMessage{
				"default": {t1, t2},
			},
			wantActive: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {},
			},
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {},
			},
			active: map[string][]*base.TaskMessage{
				"default":  {t2},
				"critical": {t3},
			},
			deadlines: map[string][]base.Z{
				"default":  {{Message: t2, Score: t2Deadline}},
				"critical": {{Message: t3, Score: t3Deadline}},
			},
			target: t3,
			wantPending: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {t3},
			},
			wantActive: map[string][]*base.TaskMessage{
				"default":  {t2},
				"critical": {},
			},
			wantDeadlines: map[string][]base.Z{
				"default":  {{Message: t2, Score: t2Deadline}},
				"critical": {},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedAllPendingQueues(t, r.client, tc.pending)
		h.SeedAllActiveQueues(t, r.client, tc.active)
		h.SeedAllDeadlines(t, r.client, tc.deadlines)

		err := r.Requeue(tc.target)
		if err != nil {
			t.Errorf("(*RDB).Requeue(task) = %v, want nil", err)
			continue
		}

		for qname, want := range tc.wantPending {
			gotPending := h.GetPendingMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.PendingKey(qname), diff)
			}
		}
		for qname, want := range tc.wantActive {
			gotActive := h.GetActiveMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotActive, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want, +got):\n%s", base.ActiveKey(qname), diff)
			}
		}
		for qname, want := range tc.wantDeadlines {
			gotDeadlines := h.GetDeadlinesEntries(t, r.client, qname)
			if diff := cmp.Diff(want, gotDeadlines, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want, +got):\n%s", base.DeadlinesKey(qname), diff)
			}
		}
	}
}

func TestSchedule(t *testing.T) {
	r := setup(t)
	defer r.Close()
	msg := h.NewTaskMessage("send_email", h.JSON(map[string]interface{}{"subject": "hello"}))
	tests := []struct {
		msg       *base.TaskMessage
		processAt time.Time
	}{
		{msg, time.Now().Add(15 * time.Minute)},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case

		err := r.Schedule(tc.msg, tc.processAt)
		if err != nil {
			t.Errorf("(*RDB).Schedule(%v, %v) = %v, want nil",
				tc.msg, tc.processAt, err)
			continue
		}

		// Check Scheduled zset has task ID.
		scheduledKey := base.ScheduledKey(tc.msg.Queue)
		zs := r.client.ZRangeWithScores(scheduledKey, 0, -1).Val()
		if n := len(zs); n != 1 {
			t.Errorf("Redis ZSET %q contains %d elements, want 1",
				scheduledKey, n)
			continue
		}
		if got := zs[0].Member.(string); got != tc.msg.ID.String() {
			t.Errorf("Redis ZSET %q member: got %v, want %v",
				scheduledKey, got, tc.msg.ID.String())
			continue
		}
		if got := int64(zs[0].Score); got != tc.processAt.Unix() {
			t.Errorf("Redis ZSET %q score: got %d, want %d",
				scheduledKey, got, tc.processAt.Unix())
			continue
		}

		// Check the values under the task key.
		taskKey := base.TaskKey(tc.msg.Queue, tc.msg.ID.String())
		encoded := r.client.HGet(taskKey, "msg").Val() // "msg" field
		decoded := h.MustUnmarshal(t, encoded)
		if diff := cmp.Diff(tc.msg, decoded); diff != "" {
			t.Errorf("persisted message was %v, want %v; (-want, +got)\n%s",
				decoded, tc.msg, diff)
		}
		state := r.client.HGet(taskKey, "state").Val() // "state" field
		if want := "scheduled"; state != want {
			t.Errorf("state field under task-key is set to %q, want %q",
				state, want)
		}
		timeout := r.client.HGet(taskKey, "timeout").Val() // "timeout" field
		if want := strconv.Itoa(int(tc.msg.Timeout)); timeout != want {
			t.Errorf("timeout field under task-key is set to %v, want %v", timeout, want)
		}
		deadline := r.client.HGet(taskKey, "deadline").Val() // "deadline" field
		if want := strconv.Itoa(int(tc.msg.Deadline)); deadline != want {
			t.Errorf("deadline field under task-ke is set to %v, want %v", deadline, want)
		}

		// Check queue is in the AllQueues set.
		if !r.client.SIsMember(base.AllQueues, tc.msg.Queue).Val() {
			t.Errorf("%q is not a member of SET %q", tc.msg.Queue, base.AllQueues)
		}
	}
}

func TestScheduleUnique(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := base.TaskMessage{
		ID:        uuid.New(),
		Type:      "email",
		Payload:   h.JSON(map[string]interface{}{"user_id": 123}),
		Queue:     base.DefaultQueueName,
		UniqueKey: base.UniqueKey(base.DefaultQueueName, "email", h.JSON(map[string]interface{}{"user_id": 123})),
	}

	tests := []struct {
		msg       *base.TaskMessage
		processAt time.Time
		ttl       time.Duration // uniqueness lock ttl
	}{
		{&m1, time.Now().Add(15 * time.Minute), time.Minute},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case

		desc := "(*RDB).ScheduleUnique(msg, processAt, ttl)"
		err := r.ScheduleUnique(tc.msg, tc.processAt, tc.ttl)
		if err != nil {
			t.Errorf("Frist task: %s = %v, want nil", desc, err)
			continue
		}

		// Check Scheduled zset has task ID.
		scheduledKey := base.ScheduledKey(tc.msg.Queue)
		zs := r.client.ZRangeWithScores(scheduledKey, 0, -1).Val()
		if n := len(zs); n != 1 {
			t.Errorf("Redis ZSET %q contains %d elements, want 1",
				scheduledKey, n)
			continue
		}
		if got := zs[0].Member.(string); got != tc.msg.ID.String() {
			t.Errorf("Redis ZSET %q member: got %v, want %v",
				scheduledKey, got, tc.msg.ID.String())
			continue
		}
		if got := int64(zs[0].Score); got != tc.processAt.Unix() {
			t.Errorf("Redis ZSET %q score: got %d, want %d",
				scheduledKey, got, tc.processAt.Unix())
			continue
		}

		// Check the values under the task key.
		taskKey := base.TaskKey(tc.msg.Queue, tc.msg.ID.String())
		encoded := r.client.HGet(taskKey, "msg").Val() // "msg" field
		decoded := h.MustUnmarshal(t, encoded)
		if diff := cmp.Diff(tc.msg, decoded); diff != "" {
			t.Errorf("persisted message was %v, want %v; (-want, +got)\n%s",
				decoded, tc.msg, diff)
		}
		state := r.client.HGet(taskKey, "state").Val() // "state" field
		if want := "scheduled"; state != want {
			t.Errorf("state field under task-key is set to %q, want %q",
				state, want)
		}
		timeout := r.client.HGet(taskKey, "timeout").Val() // "timeout" field
		if want := strconv.Itoa(int(tc.msg.Timeout)); timeout != want {
			t.Errorf("timeout field under task-key is set to %v, want %v", timeout, want)
		}
		deadline := r.client.HGet(taskKey, "deadline").Val() // "deadline" field
		if want := strconv.Itoa(int(tc.msg.Deadline)); deadline != want {
			t.Errorf("deadline field under task-key is set to %v, want %v", deadline, want)
		}
		uniqueKey := r.client.HGet(taskKey, "unique_key").Val() // "unique_key" field
		if uniqueKey != tc.msg.UniqueKey {
			t.Errorf("uniqueue_key field under task key is set to %q, want %q", uniqueKey, tc.msg.UniqueKey)
		}

		// Check queue is in the AllQueues set.
		if !r.client.SIsMember(base.AllQueues, tc.msg.Queue).Val() {
			t.Errorf("%q is not a member of SET %q", tc.msg.Queue, base.AllQueues)
		}

		// Enqueue the second message, should fail.
		got := r.ScheduleUnique(tc.msg, tc.processAt, tc.ttl)
		if !errors.Is(got, errors.ErrDuplicateTask) {
			t.Errorf("Second task: %s = %v, want %v", desc, got, errors.ErrDuplicateTask)
			continue
		}

		gotTTL := r.client.TTL(tc.msg.UniqueKey).Val()
		if !cmp.Equal(tc.ttl.Seconds(), gotTTL.Seconds(), cmpopts.EquateApprox(0, 1)) {
			t.Errorf("TTL %q = %v, want %v", tc.msg.UniqueKey, gotTTL, tc.ttl)
			continue
		}
	}
}

func TestRetry(t *testing.T) {
	r := setup(t)
	defer r.Close()
	now := time.Now()
	t1 := &base.TaskMessage{
		ID:      uuid.New(),
		Type:    "send_email",
		Payload: h.JSON(map[string]interface{}{"subject": "Hola!"}),
		Retried: 10,
		Timeout: 1800,
		Queue:   "default",
	}
	t2 := &base.TaskMessage{
		ID:      uuid.New(),
		Type:    "gen_thumbnail",
		Payload: h.JSON(map[string]interface{}{"path": "some/path/to/image.jpg"}),
		Timeout: 3000,
		Queue:   "default",
	}
	t3 := &base.TaskMessage{
		ID:      uuid.New(),
		Type:    "reindex",
		Payload: nil,
		Timeout: 60,
		Queue:   "default",
	}
	t4 := &base.TaskMessage{
		ID:      uuid.New(),
		Type:    "send_notification",
		Payload: nil,
		Timeout: 1800,
		Queue:   "custom",
	}
	t1Deadline := now.Unix() + t1.Timeout
	t2Deadline := now.Unix() + t2.Timeout
	t4Deadline := now.Unix() + t4.Timeout
	errMsg := "SMTP server is not responding"

	tests := []struct {
		active        map[string][]*base.TaskMessage
		deadlines     map[string][]base.Z
		retry         map[string][]base.Z
		msg           *base.TaskMessage
		processAt     time.Time
		errMsg        string
		wantActive    map[string][]*base.TaskMessage
		wantDeadlines map[string][]base.Z
		getWantRetry  func(failedAt time.Time) map[string][]base.Z
	}{
		{
			active: map[string][]*base.TaskMessage{
				"default": {t1, t2},
			},
			deadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}, {Message: t2, Score: t2Deadline}},
			},
			retry: map[string][]base.Z{
				"default": {{Message: t3, Score: now.Add(time.Minute).Unix()}},
			},
			msg:       t1,
			processAt: now.Add(5 * time.Minute),
			errMsg:    errMsg,
			wantActive: map[string][]*base.TaskMessage{
				"default": {t2},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {{Message: t2, Score: t2Deadline}},
			},
			getWantRetry: func(failedAt time.Time) map[string][]base.Z {
				return map[string][]base.Z{
					"default": {
						{Message: h.TaskMessageAfterRetry(*t1, errMsg, failedAt), Score: now.Add(5 * time.Minute).Unix()},
						{Message: t3, Score: now.Add(time.Minute).Unix()},
					},
				}
			},
		},
		{
			active: map[string][]*base.TaskMessage{
				"default": {t1, t2},
				"custom":  {t4},
			},
			deadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}, {Message: t2, Score: t2Deadline}},
				"custom":  {{Message: t4, Score: t4Deadline}},
			},
			retry: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			msg:       t4,
			processAt: now.Add(5 * time.Minute),
			errMsg:    errMsg,
			wantActive: map[string][]*base.TaskMessage{
				"default": {t1, t2},
				"custom":  {},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}, {Message: t2, Score: t2Deadline}},
				"custom":  {},
			},
			getWantRetry: func(failedAt time.Time) map[string][]base.Z {
				return map[string][]base.Z{
					"default": {},
					"custom": {
						{Message: h.TaskMessageAfterRetry(*t4, errMsg, failedAt), Score: now.Add(5 * time.Minute).Unix()},
					},
				}
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)
		h.SeedAllActiveQueues(t, r.client, tc.active)
		h.SeedAllDeadlines(t, r.client, tc.deadlines)
		h.SeedAllRetryQueues(t, r.client, tc.retry)

		callTime := time.Now() // time when method was called
		err := r.Retry(tc.msg, tc.processAt, tc.errMsg, true /*isFailure*/)
		if err != nil {
			t.Errorf("(*RDB).Retry = %v, want nil", err)
			continue
		}

		for queue, want := range tc.wantActive {
			gotActive := h.GetActiveMessages(t, r.client, queue)
			if diff := cmp.Diff(want, gotActive, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.ActiveKey(queue), diff)
			}
		}
		for queue, want := range tc.wantDeadlines {
			gotDeadlines := h.GetDeadlinesEntries(t, r.client, queue)
			if diff := cmp.Diff(want, gotDeadlines, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.DeadlinesKey(queue), diff)
			}
		}
		cmpOpts := []cmp.Option{
			h.SortZSetEntryOpt,
			cmpopts.EquateApproxTime(5 * time.Second), // for LastFailedAt field
		}
		wantRetry := tc.getWantRetry(callTime)
		for queue, want := range wantRetry {
			gotRetry := h.GetRetryEntries(t, r.client, queue)
			if diff := cmp.Diff(want, gotRetry, cmpOpts...); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.RetryKey(queue), diff)
			}
		}

		processedKey := base.ProcessedKey(tc.msg.Queue, time.Now())
		gotProcessed := r.client.Get(processedKey).Val()
		if gotProcessed != "1" {
			t.Errorf("GET %q = %q, want 1", processedKey, gotProcessed)
		}
		gotTTL := r.client.TTL(processedKey).Val()
		if gotTTL > statsTTL {
			t.Errorf("TTL %q = %v, want less than or equal to %v", processedKey, gotTTL, statsTTL)
		}

		failedKey := base.FailedKey(tc.msg.Queue, time.Now())
		gotFailed := r.client.Get(failedKey).Val()
		if gotFailed != "1" {
			t.Errorf("GET %q = %q, want 1", failedKey, gotFailed)
		}
		gotTTL = r.client.TTL(failedKey).Val()
		if gotTTL > statsTTL {
			t.Errorf("TTL %q = %v, want less than or equal to %v", failedKey, gotTTL, statsTTL)
		}
	}
}

func TestRetryWithNonFailureError(t *testing.T) {
	r := setup(t)
	defer r.Close()
	now := time.Now()
	t1 := &base.TaskMessage{
		ID:      uuid.New(),
		Type:    "send_email",
		Payload: h.JSON(map[string]interface{}{"subject": "Hola!"}),
		Retried: 10,
		Timeout: 1800,
		Queue:   "default",
	}
	t2 := &base.TaskMessage{
		ID:      uuid.New(),
		Type:    "gen_thumbnail",
		Payload: h.JSON(map[string]interface{}{"path": "some/path/to/image.jpg"}),
		Timeout: 3000,
		Queue:   "default",
	}
	t3 := &base.TaskMessage{
		ID:      uuid.New(),
		Type:    "reindex",
		Payload: nil,
		Timeout: 60,
		Queue:   "default",
	}
	t4 := &base.TaskMessage{
		ID:      uuid.New(),
		Type:    "send_notification",
		Payload: nil,
		Timeout: 1800,
		Queue:   "custom",
	}
	t1Deadline := now.Unix() + t1.Timeout
	t2Deadline := now.Unix() + t2.Timeout
	t4Deadline := now.Unix() + t4.Timeout
	errMsg := "SMTP server is not responding"

	tests := []struct {
		active        map[string][]*base.TaskMessage
		deadlines     map[string][]base.Z
		retry         map[string][]base.Z
		msg           *base.TaskMessage
		processAt     time.Time
		errMsg        string
		wantActive    map[string][]*base.TaskMessage
		wantDeadlines map[string][]base.Z
		getWantRetry  func(failedAt time.Time) map[string][]base.Z
	}{
		{
			active: map[string][]*base.TaskMessage{
				"default": {t1, t2},
			},
			deadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}, {Message: t2, Score: t2Deadline}},
			},
			retry: map[string][]base.Z{
				"default": {{Message: t3, Score: now.Add(time.Minute).Unix()}},
			},
			msg:       t1,
			processAt: now.Add(5 * time.Minute),
			errMsg:    errMsg,
			wantActive: map[string][]*base.TaskMessage{
				"default": {t2},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {{Message: t2, Score: t2Deadline}},
			},
			getWantRetry: func(failedAt time.Time) map[string][]base.Z {
				return map[string][]base.Z{
					"default": {
						// Task message should include the error message but without incrementing the retry count.
						{Message: h.TaskMessageWithError(*t1, errMsg, failedAt), Score: now.Add(5 * time.Minute).Unix()},
						{Message: t3, Score: now.Add(time.Minute).Unix()},
					},
				}
			},
		},
		{
			active: map[string][]*base.TaskMessage{
				"default": {t1, t2},
				"custom":  {t4},
			},
			deadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}, {Message: t2, Score: t2Deadline}},
				"custom":  {{Message: t4, Score: t4Deadline}},
			},
			retry: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			msg:       t4,
			processAt: now.Add(5 * time.Minute),
			errMsg:    errMsg,
			wantActive: map[string][]*base.TaskMessage{
				"default": {t1, t2},
				"custom":  {},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}, {Message: t2, Score: t2Deadline}},
				"custom":  {},
			},
			getWantRetry: func(failedAt time.Time) map[string][]base.Z {
				return map[string][]base.Z{
					"default": {},
					"custom": {
						// Task message should include the error message but without incrementing the retry count.
						{Message: h.TaskMessageWithError(*t4, errMsg, failedAt), Score: now.Add(5 * time.Minute).Unix()},
					},
				}
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)
		h.SeedAllActiveQueues(t, r.client, tc.active)
		h.SeedAllDeadlines(t, r.client, tc.deadlines)
		h.SeedAllRetryQueues(t, r.client, tc.retry)

		callTime := time.Now() // time when method was called
		err := r.Retry(tc.msg, tc.processAt, tc.errMsg, false /*isFailure*/)
		if err != nil {
			t.Errorf("(*RDB).Retry = %v, want nil", err)
			continue
		}

		for queue, want := range tc.wantActive {
			gotActive := h.GetActiveMessages(t, r.client, queue)
			if diff := cmp.Diff(want, gotActive, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.ActiveKey(queue), diff)
			}
		}
		for queue, want := range tc.wantDeadlines {
			gotDeadlines := h.GetDeadlinesEntries(t, r.client, queue)
			if diff := cmp.Diff(want, gotDeadlines, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.DeadlinesKey(queue), diff)
			}
		}
		cmpOpts := []cmp.Option{
			h.SortZSetEntryOpt,
			cmpopts.EquateApproxTime(5 * time.Second), // for LastFailedAt field
		}
		wantRetry := tc.getWantRetry(callTime)
		for queue, want := range wantRetry {
			gotRetry := h.GetRetryEntries(t, r.client, queue)
			if diff := cmp.Diff(want, gotRetry, cmpOpts...); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.RetryKey(queue), diff)
			}
		}

		// If isFailure is set to false, no stats should be recorded to avoid skewing the error rate.
		processedKey := base.ProcessedKey(tc.msg.Queue, time.Now())
		gotProcessed := r.client.Get(processedKey).Val()
		if gotProcessed != "" {
			t.Errorf("GET %q = %q, want empty", processedKey, gotProcessed)
		}

		// If isFailure is set to false, no stats should be recorded to avoid skewing the error rate.
		failedKey := base.FailedKey(tc.msg.Queue, time.Now())
		gotFailed := r.client.Get(failedKey).Val()
		if gotFailed != "" {
			t.Errorf("GET %q = %q, want empty", failedKey, gotFailed)
		}
	}
}

func TestArchive(t *testing.T) {
	r := setup(t)
	defer r.Close()
	now := time.Now()
	t1 := &base.TaskMessage{
		ID:      uuid.New(),
		Type:    "send_email",
		Payload: nil,
		Queue:   "default",
		Retry:   25,
		Retried: 25,
		Timeout: 1800,
	}
	t1Deadline := now.Unix() + t1.Timeout
	t2 := &base.TaskMessage{
		ID:      uuid.New(),
		Type:    "reindex",
		Payload: nil,
		Queue:   "default",
		Retry:   25,
		Retried: 0,
		Timeout: 3000,
	}
	t2Deadline := now.Unix() + t2.Timeout
	t3 := &base.TaskMessage{
		ID:      uuid.New(),
		Type:    "generate_csv",
		Payload: nil,
		Queue:   "default",
		Retry:   25,
		Retried: 0,
		Timeout: 60,
	}
	t3Deadline := now.Unix() + t3.Timeout
	t4 := &base.TaskMessage{
		ID:      uuid.New(),
		Type:    "send_email",
		Payload: nil,
		Queue:   "custom",
		Retry:   25,
		Retried: 25,
		Timeout: 1800,
	}
	t4Deadline := now.Unix() + t4.Timeout
	errMsg := "SMTP server not responding"

	// TODO(hibiken): add test cases for trimming
	tests := []struct {
		active          map[string][]*base.TaskMessage
		deadlines       map[string][]base.Z
		archived        map[string][]base.Z
		target          *base.TaskMessage // task to archive
		wantActive      map[string][]*base.TaskMessage
		wantDeadlines   map[string][]base.Z
		getWantArchived func(failedAt time.Time) map[string][]base.Z
	}{
		{
			active: map[string][]*base.TaskMessage{
				"default": {t1, t2},
			},
			deadlines: map[string][]base.Z{
				"default": {
					{Message: t1, Score: t1Deadline},
					{Message: t2, Score: t2Deadline},
				},
			},
			archived: map[string][]base.Z{
				"default": {
					{Message: t3, Score: now.Add(-time.Hour).Unix()},
				},
			},
			target: t1,
			wantActive: map[string][]*base.TaskMessage{
				"default": {t2},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {{Message: t2, Score: t2Deadline}},
			},
			getWantArchived: func(failedAt time.Time) map[string][]base.Z {
				return map[string][]base.Z{
					"default": {
						{Message: h.TaskMessageWithError(*t1, errMsg, failedAt), Score: failedAt.Unix()},
						{Message: t3, Score: now.Add(-time.Hour).Unix()},
					},
				}
			},
		},
		{
			active: map[string][]*base.TaskMessage{
				"default": {t1, t2, t3},
			},
			deadlines: map[string][]base.Z{
				"default": {
					{Message: t1, Score: t1Deadline},
					{Message: t2, Score: t2Deadline},
					{Message: t3, Score: t3Deadline},
				},
			},
			archived: map[string][]base.Z{
				"default": {},
			},
			target: t1,
			wantActive: map[string][]*base.TaskMessage{
				"default": {t2, t3},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {
					{Message: t2, Score: t2Deadline},
					{Message: t3, Score: t3Deadline},
				},
			},
			getWantArchived: func(failedAt time.Time) map[string][]base.Z {
				return map[string][]base.Z{
					"default": {
						{Message: h.TaskMessageWithError(*t1, errMsg, failedAt), Score: failedAt.Unix()},
					},
				}
			},
		},
		{
			active: map[string][]*base.TaskMessage{
				"default": {t1},
				"custom":  {t4},
			},
			deadlines: map[string][]base.Z{
				"default": {
					{Message: t1, Score: t1Deadline},
				},
				"custom": {
					{Message: t4, Score: t4Deadline},
				},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			target: t4,
			wantActive: map[string][]*base.TaskMessage{
				"default": {t1},
				"custom":  {},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}},
				"custom":  {},
			},
			getWantArchived: func(failedAt time.Time) map[string][]base.Z {
				return map[string][]base.Z{
					"default": {},
					"custom": {
						{Message: h.TaskMessageWithError(*t4, errMsg, failedAt), Score: failedAt.Unix()},
					},
				}
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedAllActiveQueues(t, r.client, tc.active)
		h.SeedAllDeadlines(t, r.client, tc.deadlines)
		h.SeedAllArchivedQueues(t, r.client, tc.archived)

		callTime := time.Now() // record time `Archive` was called
		err := r.Archive(tc.target, errMsg)
		if err != nil {
			t.Errorf("(*RDB).Archive(%v, %v) = %v, want nil", tc.target, errMsg, err)
			continue
		}

		for queue, want := range tc.wantActive {
			gotActive := h.GetActiveMessages(t, r.client, queue)
			if diff := cmp.Diff(want, gotActive, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want, +got)\n%s", base.ActiveKey(queue), diff)
			}
		}
		for queue, want := range tc.wantDeadlines {
			gotDeadlines := h.GetDeadlinesEntries(t, r.client, queue)
			if diff := cmp.Diff(want, gotDeadlines, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("mismatch found in %q after calling (*RDB).Archive: (-want, +got):\n%s", base.DeadlinesKey(queue), diff)
			}
		}
		for queue, want := range tc.getWantArchived(callTime) {
			gotArchived := h.GetArchivedEntries(t, r.client, queue)
			if diff := cmp.Diff(want, gotArchived, h.SortZSetEntryOpt, zScoreCmpOpt, timeCmpOpt); diff != "" {
				t.Errorf("mismatch found in %q after calling (*RDB).Archive: (-want, +got):\n%s", base.ArchivedKey(queue), diff)
			}
		}

		processedKey := base.ProcessedKey(tc.target.Queue, time.Now())
		gotProcessed := r.client.Get(processedKey).Val()
		if gotProcessed != "1" {
			t.Errorf("GET %q = %q, want 1", processedKey, gotProcessed)
		}
		gotTTL := r.client.TTL(processedKey).Val()
		if gotTTL > statsTTL {
			t.Errorf("TTL %q = %v, want less than or equal to %v", processedKey, gotTTL, statsTTL)
		}

		failedKey := base.FailedKey(tc.target.Queue, time.Now())
		gotFailed := r.client.Get(failedKey).Val()
		if gotFailed != "1" {
			t.Errorf("GET %q = %q, want 1", failedKey, gotFailed)
		}
		gotTTL = r.client.TTL(processedKey).Val()
		if gotTTL > statsTTL {
			t.Errorf("TTL %q = %v, want less than or equal to %v", failedKey, gotTTL, statsTTL)
		}
	}
}

func TestForwardIfReady(t *testing.T) {
	r := setup(t)
	defer r.Close()
	t1 := h.NewTaskMessage("send_email", nil)
	t2 := h.NewTaskMessage("generate_csv", nil)
	t3 := h.NewTaskMessage("gen_thumbnail", nil)
	t4 := h.NewTaskMessageWithQueue("important_task", nil, "critical")
	t5 := h.NewTaskMessageWithQueue("minor_task", nil, "low")
	secondAgo := time.Now().Add(-time.Second)
	hourFromNow := time.Now().Add(time.Hour)

	tests := []struct {
		scheduled     map[string][]base.Z
		retry         map[string][]base.Z
		qnames        []string
		wantPending   map[string][]*base.TaskMessage
		wantScheduled map[string][]*base.TaskMessage
		wantRetry     map[string][]*base.TaskMessage
	}{
		{
			scheduled: map[string][]base.Z{
				"default": {
					{Message: t1, Score: secondAgo.Unix()},
					{Message: t2, Score: secondAgo.Unix()},
				},
			},
			retry: map[string][]base.Z{
				"default": {{Message: t3, Score: secondAgo.Unix()}},
			},
			qnames: []string{"default"},
			wantPending: map[string][]*base.TaskMessage{
				"default": {t1, t2, t3},
			},
			wantScheduled: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantRetry: map[string][]*base.TaskMessage{
				"default": {},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {
					{Message: t1, Score: hourFromNow.Unix()},
					{Message: t2, Score: secondAgo.Unix()},
				},
			},
			retry: map[string][]base.Z{
				"default": {{Message: t3, Score: secondAgo.Unix()}},
			},
			qnames: []string{"default"},
			wantPending: map[string][]*base.TaskMessage{
				"default": {t2, t3},
			},
			wantScheduled: map[string][]*base.TaskMessage{
				"default": {t1},
			},
			wantRetry: map[string][]*base.TaskMessage{
				"default": {},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {
					{Message: t1, Score: hourFromNow.Unix()},
					{Message: t2, Score: hourFromNow.Unix()},
				},
			},
			retry: map[string][]base.Z{
				"default": {{Message: t3, Score: hourFromNow.Unix()}},
			},
			qnames: []string{"default"},
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantScheduled: map[string][]*base.TaskMessage{
				"default": {t1, t2},
			},
			wantRetry: map[string][]*base.TaskMessage{
				"default": {t3},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default":  {{Message: t1, Score: secondAgo.Unix()}},
				"critical": {{Message: t4, Score: secondAgo.Unix()}},
				"low":      {},
			},
			retry: map[string][]base.Z{
				"default":  {},
				"critical": {},
				"low":      {{Message: t5, Score: secondAgo.Unix()}},
			},
			qnames: []string{"default", "critical", "low"},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {t4},
				"low":      {t5},
			},
			wantScheduled: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			wantRetry: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedAllScheduledQueues(t, r.client, tc.scheduled)
		h.SeedAllRetryQueues(t, r.client, tc.retry)

		err := r.ForwardIfReady(tc.qnames...)
		if err != nil {
			t.Errorf("(*RDB).CheckScheduled(%v) = %v, want nil", tc.qnames, err)
			continue
		}

		for qname, want := range tc.wantPending {
			gotPending := h.GetPendingMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.PendingKey(qname), diff)
			}
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := h.GetScheduledMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotScheduled, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.ScheduledKey(qname), diff)
			}
		}
		for qname, want := range tc.wantRetry {
			gotRetry := h.GetRetryMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotRetry, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.RetryKey(qname), diff)
			}
		}
	}
}

func TestListDeadlineExceeded(t *testing.T) {
	t1 := h.NewTaskMessageWithQueue("task1", nil, "default")
	t2 := h.NewTaskMessageWithQueue("task2", nil, "default")
	t3 := h.NewTaskMessageWithQueue("task3", nil, "critical")

	now := time.Now()
	oneHourFromNow := now.Add(1 * time.Hour)
	fiveMinutesFromNow := now.Add(5 * time.Minute)
	fiveMinutesAgo := now.Add(-5 * time.Minute)
	oneHourAgo := now.Add(-1 * time.Hour)

	tests := []struct {
		desc      string
		deadlines map[string][]base.Z
		qnames    []string
		t         time.Time
		want      []*base.TaskMessage
	}{
		{
			desc: "with a single active task",
			deadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: fiveMinutesAgo.Unix()}},
			},
			qnames: []string{"default"},
			t:      time.Now(),
			want:   []*base.TaskMessage{t1},
		},
		{
			desc: "with multiple active tasks, and one expired",
			deadlines: map[string][]base.Z{
				"default": {
					{Message: t1, Score: oneHourAgo.Unix()},
					{Message: t2, Score: fiveMinutesFromNow.Unix()},
				},
				"critical": {
					{Message: t3, Score: oneHourFromNow.Unix()},
				},
			},
			qnames: []string{"default", "critical"},
			t:      time.Now(),
			want:   []*base.TaskMessage{t1},
		},
		{
			desc: "with multiple expired active tasks",
			deadlines: map[string][]base.Z{
				"default": {
					{Message: t1, Score: oneHourAgo.Unix()},
					{Message: t2, Score: oneHourFromNow.Unix()},
				},
				"critical": {
					{Message: t3, Score: fiveMinutesAgo.Unix()},
				},
			},
			qnames: []string{"default", "critical"},
			t:      time.Now(),
			want:   []*base.TaskMessage{t1, t3},
		},
		{
			desc: "with empty active queue",
			deadlines: map[string][]base.Z{
				"default":  {},
				"critical": {},
			},
			qnames: []string{"default", "critical"},
			t:      time.Now(),
			want:   []*base.TaskMessage{},
		},
	}

	r := setup(t)
	defer r.Close()
	for _, tc := range tests {
		h.FlushDB(t, r.client)
		h.SeedAllDeadlines(t, r.client, tc.deadlines)

		got, err := r.ListDeadlineExceeded(tc.t, tc.qnames...)
		if err != nil {
			t.Errorf("%s; ListDeadlineExceeded(%v) returned error: %v", tc.desc, tc.t, err)
			continue
		}

		if diff := cmp.Diff(tc.want, got, h.SortMsgOpt); diff != "" {
			t.Errorf("%s; ListDeadlineExceeded(%v) returned %v, want %v;(-want,+got)\n%s",
				tc.desc, tc.t, got, tc.want, diff)
		}
	}
}

func TestWriteServerState(t *testing.T) {
	r := setup(t)
	defer r.Close()

	var (
		host     = "localhost"
		pid      = 4242
		serverID = "server123"

		ttl = 5 * time.Second
	)

	info := base.ServerInfo{
		Host:              host,
		PID:               pid,
		ServerID:          serverID,
		Concurrency:       10,
		Queues:            map[string]int{"default": 2, "email": 5, "low": 1},
		StrictPriority:    false,
		Started:           time.Now().UTC(),
		Status:            "active",
		ActiveWorkerCount: 0,
	}

	err := r.WriteServerState(&info, nil /* workers */, ttl)
	if err != nil {
		t.Errorf("r.WriteServerState returned an error: %v", err)
	}

	// Check ServerInfo was written correctly.
	skey := base.ServerInfoKey(host, pid, serverID)
	data := r.client.Get(skey).Val()
	got, err := base.DecodeServerInfo([]byte(data))
	if err != nil {
		t.Fatalf("could not decode server info: %v", err)
	}
	if diff := cmp.Diff(info, *got); diff != "" {
		t.Errorf("persisted ServerInfo was %v, want %v; (-want,+got)\n%s",
			got, info, diff)
	}
	// Check ServerInfo TTL was set correctly.
	gotTTL := r.client.TTL(skey).Val()
	if !cmp.Equal(ttl.Seconds(), gotTTL.Seconds(), cmpopts.EquateApprox(0, 1)) {
		t.Errorf("TTL of %q was %v, want %v", skey, gotTTL, ttl)
	}
	// Check ServerInfo key was added to the set all server keys correctly.
	gotServerKeys := r.client.ZRange(base.AllServers, 0, -1).Val()
	wantServerKeys := []string{skey}
	if diff := cmp.Diff(wantServerKeys, gotServerKeys); diff != "" {
		t.Errorf("%q contained %v, want %v", base.AllServers, gotServerKeys, wantServerKeys)
	}

	// Check WorkersInfo was written correctly.
	wkey := base.WorkersKey(host, pid, serverID)
	workerExist := r.client.Exists(wkey).Val()
	if workerExist != 0 {
		t.Errorf("%q key exists", wkey)
	}
	// Check WorkersInfo key was added to the set correctly.
	gotWorkerKeys := r.client.ZRange(base.AllWorkers, 0, -1).Val()
	wantWorkerKeys := []string{wkey}
	if diff := cmp.Diff(wantWorkerKeys, gotWorkerKeys); diff != "" {
		t.Errorf("%q contained %v, want %v", base.AllWorkers, gotWorkerKeys, wantWorkerKeys)
	}
}

func TestWriteServerStateWithWorkers(t *testing.T) {
	r := setup(t)
	defer r.Close()

	var (
		host     = "127.0.0.1"
		pid      = 4242
		serverID = "server123"

		msg1 = h.NewTaskMessage("send_email", h.JSON(map[string]interface{}{"user_id": "123"}))
		msg2 = h.NewTaskMessage("gen_thumbnail", h.JSON(map[string]interface{}{"path": "some/path/to/imgfile"}))

		ttl = 5 * time.Second
	)

	workers := []*base.WorkerInfo{
		{
			Host:    host,
			PID:     pid,
			ID:      msg1.ID.String(),
			Type:    msg1.Type,
			Queue:   msg1.Queue,
			Payload: msg1.Payload,
			Started: time.Now().Add(-10 * time.Second),
		},
		{
			Host:    host,
			PID:     pid,
			ID:      msg2.ID.String(),
			Type:    msg2.Type,
			Queue:   msg2.Queue,
			Payload: msg2.Payload,
			Started: time.Now().Add(-2 * time.Minute),
		},
	}

	serverInfo := base.ServerInfo{
		Host:              host,
		PID:               pid,
		ServerID:          serverID,
		Concurrency:       10,
		Queues:            map[string]int{"default": 2, "email": 5, "low": 1},
		StrictPriority:    false,
		Started:           time.Now().Add(-10 * time.Minute).UTC(),
		Status:            "active",
		ActiveWorkerCount: len(workers),
	}

	err := r.WriteServerState(&serverInfo, workers, ttl)
	if err != nil {
		t.Fatalf("r.WriteServerState returned an error: %v", err)
	}

	// Check ServerInfo was written correctly.
	skey := base.ServerInfoKey(host, pid, serverID)
	data := r.client.Get(skey).Val()
	got, err := base.DecodeServerInfo([]byte(data))
	if err != nil {
		t.Fatalf("could not decode server info: %v", err)
	}
	if diff := cmp.Diff(serverInfo, *got); diff != "" {
		t.Errorf("persisted ServerInfo was %v, want %v; (-want,+got)\n%s",
			got, serverInfo, diff)
	}
	// Check ServerInfo TTL was set correctly.
	gotTTL := r.client.TTL(skey).Val()
	if !cmp.Equal(ttl.Seconds(), gotTTL.Seconds(), cmpopts.EquateApprox(0, 1)) {
		t.Errorf("TTL of %q was %v, want %v", skey, gotTTL, ttl)
	}
	// Check ServerInfo key was added to the set correctly.
	gotServerKeys := r.client.ZRange(base.AllServers, 0, -1).Val()
	wantServerKeys := []string{skey}
	if diff := cmp.Diff(wantServerKeys, gotServerKeys); diff != "" {
		t.Errorf("%q contained %v, want %v", base.AllServers, gotServerKeys, wantServerKeys)
	}

	// Check WorkersInfo was written correctly.
	wkey := base.WorkersKey(host, pid, serverID)
	wdata := r.client.HGetAll(wkey).Val()
	if len(wdata) != 2 {
		t.Fatalf("HGETALL %q returned a hash of size %d, want 2", wkey, len(wdata))
	}
	var gotWorkers []*base.WorkerInfo
	for _, val := range wdata {
		w, err := base.DecodeWorkerInfo([]byte(val))
		if err != nil {
			t.Fatalf("could not unmarshal worker's data: %v", err)
		}
		gotWorkers = append(gotWorkers, w)
	}
	if diff := cmp.Diff(workers, gotWorkers, h.SortWorkerInfoOpt); diff != "" {
		t.Errorf("persisted workers info was %v, want %v; (-want,+got)\n%s",
			gotWorkers, workers, diff)
	}

	// Check WorkersInfo TTL was set correctly.
	gotTTL = r.client.TTL(wkey).Val()
	if !cmp.Equal(ttl.Seconds(), gotTTL.Seconds(), cmpopts.EquateApprox(0, 1)) {
		t.Errorf("TTL of %q was %v, want %v", wkey, gotTTL, ttl)
	}
	// Check WorkersInfo key was added to the set correctly.
	gotWorkerKeys := r.client.ZRange(base.AllWorkers, 0, -1).Val()
	wantWorkerKeys := []string{wkey}
	if diff := cmp.Diff(wantWorkerKeys, gotWorkerKeys); diff != "" {
		t.Errorf("%q contained %v, want %v", base.AllWorkers, gotWorkerKeys, wantWorkerKeys)
	}
}

func TestClearServerState(t *testing.T) {
	r := setup(t)
	defer r.Close()

	var (
		host     = "127.0.0.1"
		pid      = 1234
		serverID = "server123"

		otherHost     = "127.0.0.2"
		otherPID      = 9876
		otherServerID = "server987"

		msg1 = h.NewTaskMessage("send_email", h.JSON(map[string]interface{}{"user_id": "123"}))
		msg2 = h.NewTaskMessage("gen_thumbnail", h.JSON(map[string]interface{}{"path": "some/path/to/imgfile"}))

		ttl = 5 * time.Second
	)

	workers1 := []*base.WorkerInfo{
		{
			Host:    host,
			PID:     pid,
			ID:      msg1.ID.String(),
			Type:    msg1.Type,
			Queue:   msg1.Queue,
			Payload: msg1.Payload,
			Started: time.Now().Add(-10 * time.Second),
		},
	}
	serverInfo1 := base.ServerInfo{
		Host:              host,
		PID:               pid,
		ServerID:          serverID,
		Concurrency:       10,
		Queues:            map[string]int{"default": 2, "email": 5, "low": 1},
		StrictPriority:    false,
		Started:           time.Now().Add(-10 * time.Minute),
		Status:            "active",
		ActiveWorkerCount: len(workers1),
	}

	workers2 := []*base.WorkerInfo{
		{
			Host:    otherHost,
			PID:     otherPID,
			ID:      msg2.ID.String(),
			Type:    msg2.Type,
			Queue:   msg2.Queue,
			Payload: msg2.Payload,
			Started: time.Now().Add(-30 * time.Second),
		},
	}
	serverInfo2 := base.ServerInfo{
		Host:              otherHost,
		PID:               otherPID,
		ServerID:          otherServerID,
		Concurrency:       10,
		Queues:            map[string]int{"default": 2, "email": 5, "low": 1},
		StrictPriority:    false,
		Started:           time.Now().Add(-15 * time.Minute),
		Status:            "active",
		ActiveWorkerCount: len(workers2),
	}

	// Write server and workers data.
	if err := r.WriteServerState(&serverInfo1, workers1, ttl); err != nil {
		t.Fatalf("could not write server state: %v", err)
	}
	if err := r.WriteServerState(&serverInfo2, workers2, ttl); err != nil {
		t.Fatalf("could not write server state: %v", err)
	}

	err := r.ClearServerState(host, pid, serverID)
	if err != nil {
		t.Fatalf("(*RDB).ClearServerState failed: %v", err)
	}

	skey := base.ServerInfoKey(host, pid, serverID)
	wkey := base.WorkersKey(host, pid, serverID)
	otherSKey := base.ServerInfoKey(otherHost, otherPID, otherServerID)
	otherWKey := base.WorkersKey(otherHost, otherPID, otherServerID)
	// Check all keys are cleared.
	if r.client.Exists(skey).Val() != 0 {
		t.Errorf("Redis key %q exists", skey)
	}
	if r.client.Exists(wkey).Val() != 0 {
		t.Errorf("Redis key %q exists", wkey)
	}
	gotServerKeys := r.client.ZRange(base.AllServers, 0, -1).Val()
	wantServerKeys := []string{otherSKey}
	if diff := cmp.Diff(wantServerKeys, gotServerKeys); diff != "" {
		t.Errorf("%q contained %v, want %v", base.AllServers, gotServerKeys, wantServerKeys)
	}
	gotWorkerKeys := r.client.ZRange(base.AllWorkers, 0, -1).Val()
	wantWorkerKeys := []string{otherWKey}
	if diff := cmp.Diff(wantWorkerKeys, gotWorkerKeys); diff != "" {
		t.Errorf("%q contained %v, want %v", base.AllWorkers, gotWorkerKeys, wantWorkerKeys)
	}
}

func TestCancelationPubSub(t *testing.T) {
	r := setup(t)
	defer r.Close()

	pubsub, err := r.CancelationPubSub()
	if err != nil {
		t.Fatalf("(*RDB).CancelationPubSub() returned an error: %v", err)
	}

	cancelCh := pubsub.Channel()

	var (
		mu       sync.Mutex
		received []string
	)

	go func() {
		for msg := range cancelCh {
			mu.Lock()
			received = append(received, msg.Payload)
			mu.Unlock()
		}
	}()

	publish := []string{"one", "two", "three"}

	for _, msg := range publish {
		r.PublishCancelation(msg)
	}

	// allow for message to reach subscribers.
	time.Sleep(time.Second)

	pubsub.Close()

	mu.Lock()
	if diff := cmp.Diff(publish, received, h.SortStringSliceOpt); diff != "" {
		t.Errorf("subscriber received %v, want %v; (-want,+got)\n%s", received, publish, diff)
	}
	mu.Unlock()
}
