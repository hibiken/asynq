package rdb

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/hibiken/asynq/internal/base"
)

func TestEnqueue(t *testing.T) {
	r := setup(t)
	tests := []struct {
		msg *base.TaskMessage
	}{
		{msg: newTaskMessage("send_email", map[string]interface{}{"to": "exampleuser@gmail.com", "from": "noreply@example.com"})},
		{msg: newTaskMessage("generate_csv", map[string]interface{}{})},
		{msg: newTaskMessage("sync", nil)},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case.

		err := r.Enqueue(tc.msg)
		if err != nil {
			t.Errorf("(*RDB).Enqueue = %v, want nil", err)
			continue
		}
		res := r.client.LRange(base.DefaultQueue, 0, -1).Val()
		if len(res) != 1 {
			t.Errorf("%q has length %d, want 1", base.DefaultQueue, len(res))
			continue
		}
		if diff := cmp.Diff(tc.msg, mustUnmarshal(t, res[0])); diff != "" {
			t.Errorf("persisted data differed from the original input (-want, +got)\n%s", diff)
		}
	}
}

func TestDequeue(t *testing.T) {
	r := setup(t)
	t1 := newTaskMessage("send_email", map[string]interface{}{"subject": "hello!"})
	tests := []struct {
		enqueued   []*base.TaskMessage
		want       *base.TaskMessage
		err        error
		inProgress int64 // length of "in-progress" tasks after dequeue
	}{
		{enqueued: []*base.TaskMessage{t1}, want: t1, err: nil, inProgress: 1},
		{enqueued: []*base.TaskMessage{}, want: nil, err: ErrDequeueTimeout, inProgress: 0},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case
		seedDefaultQueue(t, r, tc.enqueued)

		got, err := r.Dequeue(time.Second)
		if !cmp.Equal(got, tc.want) || err != tc.err {
			t.Errorf("(*RDB).Dequeue(time.Second) = %v, %v; want %v, %v",
				got, err, tc.want, tc.err)
			continue
		}
		if l := r.client.LLen(base.InProgressQueue).Val(); l != tc.inProgress {
			t.Errorf("%q has length %d, want %d", base.InProgressQueue, l, tc.inProgress)
		}
	}
}

func TestDone(t *testing.T) {
	r := setup(t)
	t1 := newTaskMessage("send_email", nil)
	t2 := newTaskMessage("export_csv", nil)

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
			inProgress:     []*base.TaskMessage{t2},
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
		flushDB(t, r) // clean up db before each test case
		seedInProgressQueue(t, r, tc.inProgress)

		err := r.Done(tc.target)
		if err != nil {
			t.Errorf("(*RDB).Done(task) = %v, want nil", err)
			continue
		}

		data := r.client.LRange(base.InProgressQueue, 0, -1).Val()
		gotInProgress := mustUnmarshalSlice(t, data)
		if diff := cmp.Diff(tc.wantInProgress, gotInProgress, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q after calling (*RDB).Done: (-want, +got):\n%s", base.InProgressQueue, diff)
			continue
		}
	}
}

func TestRequeue(t *testing.T) {
	r := setup(t)
	t1 := newTaskMessage("send_email", nil)
	t2 := newTaskMessage("export_csv", nil)

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
		flushDB(t, r) // clean up db before each test case
		seedDefaultQueue(t, r, tc.enqueued)
		seedInProgressQueue(t, r, tc.inProgress)

		err := r.Requeue(tc.target)
		if err != nil {
			t.Errorf("(*RDB).Requeue(task) = %v, want nil", err)
			continue
		}

		gotEnqueuedRaw := r.client.LRange(base.DefaultQueue, 0, -1).Val()
		gotEnqueued := mustUnmarshalSlice(t, gotEnqueuedRaw)
		if diff := cmp.Diff(tc.wantEnqueued, gotEnqueued, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q: (-want, +got):\n%s", base.DefaultQueue, diff)
		}

		gotInProgressRaw := r.client.LRange(base.InProgressQueue, 0, -1).Val()
		gotInProgress := mustUnmarshalSlice(t, gotInProgressRaw)
		if diff := cmp.Diff(tc.wantInProgress, gotInProgress, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q: (-want, +got):\n%s", base.InProgressQueue, diff)
		}
	}
}

func TestKill(t *testing.T) {
	r := setup(t)
	t1 := newTaskMessage("send_email", nil)
	t2 := newTaskMessage("reindex", nil)
	t3 := newTaskMessage("generate_csv", nil)
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
		dead           []sortedSetEntry
		target         *base.TaskMessage // task to kill
		wantInProgress []*base.TaskMessage
		wantDead       []sortedSetEntry
	}{
		{
			inProgress: []*base.TaskMessage{t1, t2},
			dead: []sortedSetEntry{
				{t3, now.Add(-time.Hour).Unix()},
			},
			target:         t1,
			wantInProgress: []*base.TaskMessage{t2},
			wantDead: []sortedSetEntry{
				{t1AfterKill, now.Unix()},
				{t3, now.Add(-time.Hour).Unix()},
			},
		},
		{
			inProgress:     []*base.TaskMessage{t1, t2, t3},
			dead:           []sortedSetEntry{},
			target:         t1,
			wantInProgress: []*base.TaskMessage{t2, t3},
			wantDead: []sortedSetEntry{
				{t1AfterKill, now.Unix()},
			},
		},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case
		seedInProgressQueue(t, r, tc.inProgress)
		seedDeadQueue(t, r, tc.dead)

		err := r.Kill(tc.target, errMsg)
		if err != nil {
			t.Errorf("(*RDB).Kill(%v, %v) = %v, want nil", tc.target, errMsg, err)
			continue
		}

		gotInProgressRaw := r.client.LRange(base.InProgressQueue, 0, -1).Val()
		gotInProgress := mustUnmarshalSlice(t, gotInProgressRaw)
		if diff := cmp.Diff(tc.wantInProgress, gotInProgress, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.InProgressQueue, diff)
		}

		var gotDead []sortedSetEntry
		data := r.client.ZRangeWithScores(base.DeadQueue, 0, -1).Val()
		for _, z := range data {
			gotDead = append(gotDead, sortedSetEntry{
				msg:   mustUnmarshal(t, z.Member.(string)),
				score: int64(z.Score),
			})
		}

		cmpOpt := cmp.AllowUnexported(sortedSetEntry{})
		if diff := cmp.Diff(tc.wantDead, gotDead, cmpOpt, sortZSetEntryOpt); diff != "" {
			t.Errorf("mismatch found in %q after calling (*RDB).Kill: (-want, +got):\n%s", base.DeadQueue, diff)
		}
	}
}

func TestRestoreUnfinished(t *testing.T) {
	r := setup(t)
	t1 := newTaskMessage("send_email", nil)
	t2 := newTaskMessage("export_csv", nil)
	t3 := newTaskMessage("sync_stuff", nil)

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
		flushDB(t, r) // clean up db before each test case
		seedInProgressQueue(t, r, tc.inProgress)
		seedDefaultQueue(t, r, tc.enqueued)

		got, err := r.RestoreUnfinished()

		if got != tc.want || err != nil {
			t.Errorf("(*RDB).RestoreUnfinished() = %v %v, want %v nil", got, err, tc.want)
			continue
		}

		gotInProgressRaw := r.client.LRange(base.InProgressQueue, 0, -1).Val()
		gotInProgress := mustUnmarshalSlice(t, gotInProgressRaw)
		if diff := cmp.Diff(tc.wantInProgress, gotInProgress, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q (-want, +got)\n%s", base.InProgressQueue, diff)
		}
		gotEnqueuedRaw := r.client.LRange(base.DefaultQueue, 0, -1).Val()
		gotEnqueued := mustUnmarshalSlice(t, gotEnqueuedRaw)
		if diff := cmp.Diff(tc.wantEnqueued, gotEnqueued, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q (-want, +got)\n%s", base.DefaultQueue, diff)
		}
	}
}

func TestCheckAndEnqueue(t *testing.T) {
	r := setup(t)
	t1 := newTaskMessage("send_email", nil)
	t2 := newTaskMessage("generate_csv", nil)
	t3 := newTaskMessage("gen_thumbnail", nil)
	secondAgo := time.Now().Add(-time.Second)
	hourFromNow := time.Now().Add(time.Hour)

	tests := []struct {
		scheduled     []sortedSetEntry
		retry         []sortedSetEntry
		wantQueued    []*base.TaskMessage
		wantScheduled []*base.TaskMessage
		wantRetry     []*base.TaskMessage
	}{
		{
			scheduled: []sortedSetEntry{
				{t1, secondAgo.Unix()},
				{t2, secondAgo.Unix()}},
			retry: []sortedSetEntry{
				{t3, secondAgo.Unix()}},
			wantQueued:    []*base.TaskMessage{t1, t2, t3},
			wantScheduled: []*base.TaskMessage{},
			wantRetry:     []*base.TaskMessage{},
		},
		{
			scheduled: []sortedSetEntry{
				{t1, hourFromNow.Unix()},
				{t2, secondAgo.Unix()}},
			retry: []sortedSetEntry{
				{t3, secondAgo.Unix()}},
			wantQueued:    []*base.TaskMessage{t2, t3},
			wantScheduled: []*base.TaskMessage{t1},
			wantRetry:     []*base.TaskMessage{},
		},
		{
			scheduled: []sortedSetEntry{
				{t1, hourFromNow.Unix()},
				{t2, hourFromNow.Unix()}},
			retry: []sortedSetEntry{
				{t3, hourFromNow.Unix()}},
			wantQueued:    []*base.TaskMessage{},
			wantScheduled: []*base.TaskMessage{t1, t2},
			wantRetry:     []*base.TaskMessage{t3},
		},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case
		seedScheduledQueue(t, r, tc.scheduled)
		seedRetryQueue(t, r, tc.retry)

		err := r.CheckAndEnqueue()
		if err != nil {
			t.Errorf("(*RDB).CheckScheduled() = %v, want nil", err)
			continue
		}
		queued := r.client.LRange(base.DefaultQueue, 0, -1).Val()
		gotQueued := mustUnmarshalSlice(t, queued)
		if diff := cmp.Diff(tc.wantQueued, gotQueued, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.DefaultQueue, diff)
		}
		scheduled := r.client.ZRange(base.ScheduledQueue, 0, -1).Val()
		gotScheduled := mustUnmarshalSlice(t, scheduled)
		if diff := cmp.Diff(tc.wantScheduled, gotScheduled, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.ScheduledQueue, diff)
		}
		retry := r.client.ZRange(base.RetryQueue, 0, -1).Val()
		gotRetry := mustUnmarshalSlice(t, retry)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.RetryQueue, diff)
		}
	}
}

func TestSchedule(t *testing.T) {
	r := setup(t)
	tests := []struct {
		msg       *base.TaskMessage
		processAt time.Time
	}{
		{
			newTaskMessage("send_email", map[string]interface{}{"subject": "hello"}),
			time.Now().Add(15 * time.Minute),
		},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case

		desc := fmt.Sprintf("(*RDB).Schedule(%v, %v)", tc.msg, tc.processAt)
		err := r.Schedule(tc.msg, tc.processAt)
		if err != nil {
			t.Errorf("%s = %v, want nil", desc, err)
			continue
		}

		res := r.client.ZRangeWithScores(base.ScheduledQueue, 0, -1).Val()
		if len(res) != 1 {
			t.Errorf("%s inserted %d items to %q, want 1 items inserted", desc, len(res), base.ScheduledQueue)
			continue
		}
		if res[0].Score != float64(tc.processAt.Unix()) {
			t.Errorf("%s inserted an item with score %f, want %f", desc, res[0].Score, float64(tc.processAt.Unix()))
			continue
		}
	}
}

func TestRetry(t *testing.T) {
	r := setup(t)
	t1 := newTaskMessage("send_email", map[string]interface{}{"subject": "Hola!"})
	t2 := newTaskMessage("gen_thumbnail", map[string]interface{}{"path": "some/path/to/image.jpg"})
	t3 := newTaskMessage("reindex", nil)
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
		retry          []sortedSetEntry
		msg            *base.TaskMessage
		processAt      time.Time
		errMsg         string
		wantInProgress []*base.TaskMessage
		wantRetry      []sortedSetEntry
	}{
		{
			inProgress: []*base.TaskMessage{t1, t2},
			retry: []sortedSetEntry{
				{t3, now.Add(time.Minute).Unix()},
			},
			msg:            t1,
			processAt:      now.Add(5 * time.Minute),
			errMsg:         errMsg,
			wantInProgress: []*base.TaskMessage{t2},
			wantRetry: []sortedSetEntry{
				{t1AfterRetry, now.Add(5 * time.Minute).Unix()},
				{t3, now.Add(time.Minute).Unix()},
			},
		},
	}

	for _, tc := range tests {
		flushDB(t, r)
		seedInProgressQueue(t, r, tc.inProgress)
		seedRetryQueue(t, r, tc.retry)

		err := r.Retry(tc.msg, tc.processAt, tc.errMsg)
		if err != nil {
			t.Errorf("(*RDB).Retry = %v, want nil", err)
			continue
		}

		gotInProgressRaw := r.client.LRange(base.InProgressQueue, 0, -1).Val()
		gotInProgress := mustUnmarshalSlice(t, gotInProgressRaw)
		if diff := cmp.Diff(tc.wantInProgress, gotInProgress, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.InProgressQueue, diff)
		}

		gotRetryRaw := r.client.ZRangeWithScores(base.RetryQueue, 0, -1).Val()
		var gotRetry []sortedSetEntry
		for _, z := range gotRetryRaw {
			gotRetry = append(gotRetry, sortedSetEntry{
				msg:   mustUnmarshal(t, z.Member.(string)),
				score: int64(z.Score),
			})
		}
		cmpOpt := cmp.AllowUnexported(sortedSetEntry{})
		if diff := cmp.Diff(tc.wantRetry, gotRetry, cmpOpt, sortZSetEntryOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.RetryQueue, diff)
		}
	}
}
