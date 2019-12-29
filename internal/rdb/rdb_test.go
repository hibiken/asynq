package rdb

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/google/go-cmp/cmp"
	h "github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
)

// TODO(hibiken): Get Redis address and db number from ENV variables.
func setup(t *testing.T) *RDB {
	t.Helper()
	r := NewRDB(redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   13,
	}))
	// Start each test with a clean slate.
	h.FlushDB(t, r.client)
	return r
}

func TestEnqueue(t *testing.T) {
	r := setup(t)
	tests := []struct {
		msg *base.TaskMessage
	}{
		{h.NewTaskMessage("send_email", map[string]interface{}{"to": "exampleuser@gmail.com", "from": "noreply@example.com"})},
		{h.NewTaskMessage("generate_csv", map[string]interface{}{})},
		{h.NewTaskMessage("sync", nil)},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case.

		err := r.Enqueue(tc.msg)
		if err != nil {
			t.Errorf("(*RDB).Enqueue = %v, want nil", err)
			continue
		}
		gotEnqueued := h.GetEnqueuedMessages(t, r.client)
		if len(gotEnqueued) != 1 {
			t.Errorf("%q has length %d, want 1", base.DefaultQueue, len(gotEnqueued))
			continue
		}
		if diff := cmp.Diff(tc.msg, gotEnqueued[0]); diff != "" {
			t.Errorf("persisted data differed from the original input (-want, +got)\n%s", diff)
		}
	}
}

func TestDequeue(t *testing.T) {
	r := setup(t)
	t1 := h.NewTaskMessage("send_email", map[string]interface{}{"subject": "hello!"})
	tests := []struct {
		enqueued       []*base.TaskMessage
		want           *base.TaskMessage
		err            error
		wantInProgress []*base.TaskMessage
	}{
		{
			enqueued:       []*base.TaskMessage{t1},
			want:           t1,
			err:            nil,
			wantInProgress: []*base.TaskMessage{t1},
		},
		{
			enqueued:       []*base.TaskMessage{},
			want:           nil,
			err:            ErrDequeueTimeout,
			wantInProgress: []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedDefaultQueue(t, r.client, tc.enqueued)

		got, err := r.Dequeue(time.Second)
		if !cmp.Equal(got, tc.want) || err != tc.err {
			t.Errorf("(*RDB).Dequeue(time.Second) = %v, %v; want %v, %v",
				got, err, tc.want, tc.err)
			continue
		}

		gotInProgress := h.GetInProgressMessages(t, r.client)
		if diff := cmp.Diff(tc.wantInProgress, gotInProgress, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q: (-want,+got):\n%s", base.InProgressQueue, diff)
		}
	}
}

func TestDone(t *testing.T) {
	r := setup(t)
	t1 := h.NewTaskMessage("send_email", nil)
	t2 := h.NewTaskMessage("export_csv", nil)

	tests := []struct {
		inProgress     []*base.TaskMessage // initial state of the in-progress list
		target         *base.TaskMessage   // task to remove
		wantInProgress []*base.TaskMessage // final state of the in-progress list
	}{
		{
			inProgress:     []*base.TaskMessage{t1, t2},
			target:         t1,
			wantInProgress: []*base.TaskMessage{t2},
		},
		{
			inProgress:     []*base.TaskMessage{t1},
			target:         t1,
			wantInProgress: []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedInProgressQueue(t, r.client, tc.inProgress)

		err := r.Done(tc.target)
		if err != nil {
			t.Errorf("(*RDB).Done(task) = %v, want nil", err)
			continue
		}

		gotInProgress := h.GetInProgressMessages(t, r.client)
		if diff := cmp.Diff(tc.wantInProgress, gotInProgress, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q: (-want, +got):\n%s", base.InProgressQueue, diff)
			continue
		}

		processedKey := base.ProcessedKey(time.Now())
		gotProcessed := r.client.Get(processedKey).Val()
		if gotProcessed != "1" {
			t.Errorf("GET %q = %q, want 1", processedKey, gotProcessed)
		}

		gotTTL := r.client.TTL(processedKey).Val()
		if gotTTL > statsTTL {
			t.Errorf("TTL %q = %v, want less than or equal to %v", processedKey, gotTTL, statsTTL)
		}
	}
}

func TestRequeue(t *testing.T) {
	r := setup(t)
	t1 := h.NewTaskMessage("send_email", nil)
	t2 := h.NewTaskMessage("export_csv", nil)

	tests := []struct {
		enqueued       []*base.TaskMessage // initial state of the default queue
		inProgress     []*base.TaskMessage // initial state of the in-progress list
		target         *base.TaskMessage   // task to requeue
		wantEnqueued   []*base.TaskMessage // final state of the default queue
		wantInProgress []*base.TaskMessage // final state of the in-progress list
	}{
		{
			enqueued:       []*base.TaskMessage{},
			inProgress:     []*base.TaskMessage{t1, t2},
			target:         t1,
			wantEnqueued:   []*base.TaskMessage{t1},
			wantInProgress: []*base.TaskMessage{t2},
		},
		{
			enqueued:       []*base.TaskMessage{t1},
			inProgress:     []*base.TaskMessage{t2},
			target:         t2,
			wantEnqueued:   []*base.TaskMessage{t1, t2},
			wantInProgress: []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedDefaultQueue(t, r.client, tc.enqueued)
		h.SeedInProgressQueue(t, r.client, tc.inProgress)

		err := r.Requeue(tc.target)
		if err != nil {
			t.Errorf("(*RDB).Requeue(task) = %v, want nil", err)
			continue
		}

		gotEnqueued := h.GetEnqueuedMessages(t, r.client)
		if diff := cmp.Diff(tc.wantEnqueued, gotEnqueued, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q: (-want, +got):\n%s", base.DefaultQueue, diff)
		}

		gotInProgress := h.GetInProgressMessages(t, r.client)
		if diff := cmp.Diff(tc.wantInProgress, gotInProgress, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q: (-want, +got):\n%s", base.InProgressQueue, diff)
		}
	}
}

func TestSchedule(t *testing.T) {
	r := setup(t)
	t1 := h.NewTaskMessage("send_email", map[string]interface{}{"subject": "hello"})
	tests := []struct {
		msg       *base.TaskMessage
		processAt time.Time
	}{
		{t1, time.Now().Add(15 * time.Minute)},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case

		desc := fmt.Sprintf("(*RDB).Schedule(%v, %v)", tc.msg, tc.processAt)
		err := r.Schedule(tc.msg, tc.processAt)
		if err != nil {
			t.Errorf("%s = %v, want nil", desc, err)
			continue
		}

		gotScheduled := h.GetScheduledEntries(t, r.client)
		if len(gotScheduled) != 1 {
			t.Errorf("%s inserted %d items to %q, want 1 items inserted", desc, len(gotScheduled), base.ScheduledQueue)
			continue
		}
		if gotScheduled[0].Score != tc.processAt.Unix() {
			t.Errorf("%s inserted an item with score %d, want %d", desc, gotScheduled[0].Score, tc.processAt.Unix())
			continue
		}
	}
}

func TestRetry(t *testing.T) {
	r := setup(t)
	t1 := h.NewTaskMessage("send_email", map[string]interface{}{"subject": "Hola!"})
	t2 := h.NewTaskMessage("gen_thumbnail", map[string]interface{}{"path": "some/path/to/image.jpg"})
	t3 := h.NewTaskMessage("reindex", nil)
	t1.Retried = 10
	errMsg := "SMTP server is not responding"
	t1AfterRetry := &base.TaskMessage{
		ID:       t1.ID,
		Type:     t1.Type,
		Payload:  t1.Payload,
		Queue:    t1.Queue,
		Retry:    t1.Retry,
		Retried:  t1.Retried + 1,
		ErrorMsg: errMsg,
	}
	now := time.Now()

	tests := []struct {
		inProgress     []*base.TaskMessage
		retry          []h.ZSetEntry
		msg            *base.TaskMessage
		processAt      time.Time
		errMsg         string
		wantInProgress []*base.TaskMessage
		wantRetry      []h.ZSetEntry
	}{
		{
			inProgress: []*base.TaskMessage{t1, t2},
			retry: []h.ZSetEntry{
				{
					Msg:   t3,
					Score: now.Add(time.Minute).Unix(),
				},
			},
			msg:            t1,
			processAt:      now.Add(5 * time.Minute),
			errMsg:         errMsg,
			wantInProgress: []*base.TaskMessage{t2},
			wantRetry: []h.ZSetEntry{
				{
					Msg:   t1AfterRetry,
					Score: now.Add(5 * time.Minute).Unix(),
				},
				{
					Msg:   t3,
					Score: now.Add(time.Minute).Unix(),
				},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)
		h.SeedInProgressQueue(t, r.client, tc.inProgress)
		h.SeedRetryQueue(t, r.client, tc.retry)

		err := r.Retry(tc.msg, tc.processAt, tc.errMsg)
		if err != nil {
			t.Errorf("(*RDB).Retry = %v, want nil", err)
			continue
		}

		gotInProgress := h.GetInProgressMessages(t, r.client)
		if diff := cmp.Diff(tc.wantInProgress, gotInProgress, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.InProgressQueue, diff)
		}

		gotRetry := h.GetRetryEntries(t, r.client)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, h.SortZSetEntryOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.RetryQueue, diff)
		}

		processedKey := base.ProcessedKey(time.Now())
		gotProcessed := r.client.Get(processedKey).Val()
		if gotProcessed != "1" {
			t.Errorf("GET %q = %q, want 1", processedKey, gotProcessed)
		}
		gotTTL := r.client.TTL(processedKey).Val()
		if gotTTL > statsTTL {
			t.Errorf("TTL %q = %v, want less than or equal to %v", processedKey, gotTTL, statsTTL)
		}

		failureKey := base.FailureKey(time.Now())
		gotFailure := r.client.Get(failureKey).Val()
		if gotFailure != "1" {
			t.Errorf("GET %q = %q, want 1", failureKey, gotFailure)
		}
		gotTTL = r.client.TTL(processedKey).Val()
		if gotTTL > statsTTL {
			t.Errorf("TTL %q = %v, want less than or equal to %v", failureKey, gotTTL, statsTTL)
		}
	}
}
func TestKill(t *testing.T) {
	r := setup(t)
	t1 := h.NewTaskMessage("send_email", nil)
	t2 := h.NewTaskMessage("reindex", nil)
	t3 := h.NewTaskMessage("generate_csv", nil)
	errMsg := "SMTP server not responding"
	t1AfterKill := &base.TaskMessage{
		ID:       t1.ID,
		Type:     t1.Type,
		Payload:  t1.Payload,
		Queue:    t1.Queue,
		Retry:    t1.Retry,
		Retried:  t1.Retried,
		ErrorMsg: errMsg,
	}
	now := time.Now()

	// TODO(hibiken): add test cases for trimming
	tests := []struct {
		inProgress     []*base.TaskMessage
		dead           []h.ZSetEntry
		target         *base.TaskMessage // task to kill
		wantInProgress []*base.TaskMessage
		wantDead       []h.ZSetEntry
	}{
		{
			inProgress: []*base.TaskMessage{t1, t2},
			dead: []h.ZSetEntry{
				{
					Msg:   t3,
					Score: now.Add(-time.Hour).Unix(),
				},
			},
			target:         t1,
			wantInProgress: []*base.TaskMessage{t2},
			wantDead: []h.ZSetEntry{
				{
					Msg:   t1AfterKill,
					Score: now.Unix(),
				},
				{
					Msg:   t3,
					Score: now.Add(-time.Hour).Unix(),
				},
			},
		},
		{
			inProgress:     []*base.TaskMessage{t1, t2, t3},
			dead:           []h.ZSetEntry{},
			target:         t1,
			wantInProgress: []*base.TaskMessage{t2, t3},
			wantDead: []h.ZSetEntry{
				{
					Msg:   t1AfterKill,
					Score: now.Unix(),
				},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedInProgressQueue(t, r.client, tc.inProgress)
		h.SeedDeadQueue(t, r.client, tc.dead)

		err := r.Kill(tc.target, errMsg)
		if err != nil {
			t.Errorf("(*RDB).Kill(%v, %v) = %v, want nil", tc.target, errMsg, err)
			continue
		}

		gotInProgress := h.GetInProgressMessages(t, r.client)
		if diff := cmp.Diff(tc.wantInProgress, gotInProgress, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q: (-want, +got)\n%s", base.InProgressQueue, diff)
		}

		gotDead := h.GetDeadEntries(t, r.client)
		if diff := cmp.Diff(tc.wantDead, gotDead, h.SortZSetEntryOpt); diff != "" {
			t.Errorf("mismatch found in %q after calling (*RDB).Kill: (-want, +got):\n%s", base.DeadQueue, diff)
		}

		processedKey := base.ProcessedKey(time.Now())
		gotProcessed := r.client.Get(processedKey).Val()
		if gotProcessed != "1" {
			t.Errorf("GET %q = %q, want 1", processedKey, gotProcessed)
		}
		gotTTL := r.client.TTL(processedKey).Val()
		if gotTTL > statsTTL {
			t.Errorf("TTL %q = %v, want less than or equal to %v", processedKey, gotTTL, statsTTL)
		}

		failureKey := base.FailureKey(time.Now())
		gotFailure := r.client.Get(failureKey).Val()
		if gotFailure != "1" {
			t.Errorf("GET %q = %q, want 1", failureKey, gotFailure)
		}
		gotTTL = r.client.TTL(processedKey).Val()
		if gotTTL > statsTTL {
			t.Errorf("TTL %q = %v, want less than or equal to %v", failureKey, gotTTL, statsTTL)
		}
	}
}

func TestRestoreUnfinished(t *testing.T) {
	r := setup(t)
	t1 := h.NewTaskMessage("send_email", nil)
	t2 := h.NewTaskMessage("export_csv", nil)
	t3 := h.NewTaskMessage("sync_stuff", nil)

	tests := []struct {
		inProgress     []*base.TaskMessage
		enqueued       []*base.TaskMessage
		want           int64
		wantInProgress []*base.TaskMessage
		wantEnqueued   []*base.TaskMessage
	}{
		{
			inProgress:     []*base.TaskMessage{t1, t2, t3},
			enqueued:       []*base.TaskMessage{},
			want:           3,
			wantInProgress: []*base.TaskMessage{},
			wantEnqueued:   []*base.TaskMessage{t1, t2, t3},
		},
		{
			inProgress:     []*base.TaskMessage{},
			enqueued:       []*base.TaskMessage{t1, t2, t3},
			want:           0,
			wantInProgress: []*base.TaskMessage{},
			wantEnqueued:   []*base.TaskMessage{t1, t2, t3},
		},
		{
			inProgress:     []*base.TaskMessage{t2, t3},
			enqueued:       []*base.TaskMessage{t1},
			want:           2,
			wantInProgress: []*base.TaskMessage{},
			wantEnqueued:   []*base.TaskMessage{t1, t2, t3},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedInProgressQueue(t, r.client, tc.inProgress)
		h.SeedDefaultQueue(t, r.client, tc.enqueued)

		got, err := r.RestoreUnfinished()
		if got != tc.want || err != nil {
			t.Errorf("(*RDB).RestoreUnfinished() = %v %v, want %v nil", got, err, tc.want)
			continue
		}

		gotInProgress := h.GetInProgressMessages(t, r.client)
		if diff := cmp.Diff(tc.wantInProgress, gotInProgress, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q: (-want, +got):\n%s", base.InProgressQueue, diff)
		}

		gotEnqueued := h.GetEnqueuedMessages(t, r.client)
		if diff := cmp.Diff(tc.wantEnqueued, gotEnqueued, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q: (-want, +got):\n%s", base.DefaultQueue, diff)
		}
	}
}

func TestCheckAndEnqueue(t *testing.T) {
	r := setup(t)
	t1 := h.NewTaskMessage("send_email", nil)
	t2 := h.NewTaskMessage("generate_csv", nil)
	t3 := h.NewTaskMessage("gen_thumbnail", nil)
	secondAgo := time.Now().Add(-time.Second)
	hourFromNow := time.Now().Add(time.Hour)

	tests := []struct {
		scheduled     []h.ZSetEntry
		retry         []h.ZSetEntry
		wantQueued    []*base.TaskMessage
		wantScheduled []*base.TaskMessage
		wantRetry     []*base.TaskMessage
	}{
		{
			scheduled: []h.ZSetEntry{
				{Msg: t1, Score: secondAgo.Unix()},
				{Msg: t2, Score: secondAgo.Unix()},
			},
			retry: []h.ZSetEntry{
				{Msg: t3, Score: secondAgo.Unix()}},
			wantQueued:    []*base.TaskMessage{t1, t2, t3},
			wantScheduled: []*base.TaskMessage{},
			wantRetry:     []*base.TaskMessage{},
		},
		{
			scheduled: []h.ZSetEntry{
				{Msg: t1, Score: hourFromNow.Unix()},
				{Msg: t2, Score: secondAgo.Unix()}},
			retry: []h.ZSetEntry{
				{Msg: t3, Score: secondAgo.Unix()}},
			wantQueued:    []*base.TaskMessage{t2, t3},
			wantScheduled: []*base.TaskMessage{t1},
			wantRetry:     []*base.TaskMessage{},
		},
		{
			scheduled: []h.ZSetEntry{
				{Msg: t1, Score: hourFromNow.Unix()},
				{Msg: t2, Score: hourFromNow.Unix()}},
			retry: []h.ZSetEntry{
				{Msg: t3, Score: hourFromNow.Unix()}},
			wantQueued:    []*base.TaskMessage{},
			wantScheduled: []*base.TaskMessage{t1, t2},
			wantRetry:     []*base.TaskMessage{t3},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedScheduledQueue(t, r.client, tc.scheduled)
		h.SeedRetryQueue(t, r.client, tc.retry)

		err := r.CheckAndEnqueue()
		if err != nil {
			t.Errorf("(*RDB).CheckScheduled() = %v, want nil", err)
			continue
		}

		gotEnqueued := h.GetEnqueuedMessages(t, r.client)
		if diff := cmp.Diff(tc.wantQueued, gotEnqueued, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.DefaultQueue, diff)
		}

		gotScheduled := h.GetScheduledMessages(t, r.client)
		if diff := cmp.Diff(tc.wantScheduled, gotScheduled, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.ScheduledQueue, diff)
		}

		gotRetry := h.GetRetryMessages(t, r.client)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.RetryQueue, diff)
		}
	}
}
