package rdb

import (
	"sort"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/google/go-cmp/cmp"
	"github.com/rs/xid"
)

// TODO(hibiken): Replace this with cmpopts.EquateApproxTime once it becomes availalble.
// https://github.com/google/go-cmp/issues/166
//
// EquateApproxTime returns a Comparer options that
// determine two time.Time values to be equal if they
// are within the given time interval of one another.
// Note that if both times have a monotonic clock reading,
// the monotonic time difference will be used.
//
// The zero time is treated specially: it is only considered
// equal to another zero time value.
//
// It will panic if margin is negative.
func EquateApproxTime(margin time.Duration) cmp.Option {
	if margin < 0 {
		panic("negative duration in EquateApproxTime")
	}
	return cmp.FilterValues(func(x, y time.Time) bool {
		return !x.IsZero() && !y.IsZero()
	}, cmp.Comparer(timeApproximator{margin}.compare))
}

type timeApproximator struct {
	margin time.Duration
}

func (a timeApproximator) compare(x, y time.Time) bool {
	// Avoid subtracting times to avoid overflow when the
	// difference is larger than the largest representible duration.
	if x.After(y) {
		// Ensure x is always before y
		x, y = y, x
	}
	// We're within the margin if x+margin >= y.
	// Note: time.Time doesn't have AfterOrEqual method hence the negation.
	return !x.Add(a.margin).Before(y)
}

func TestCurrentStats(t *testing.T) {
	r := setup(t)
	m1 := randomTask("send_email", "default", map[string]interface{}{"subject": "hello"})
	m2 := randomTask("reindex", "default", nil)
	m3 := randomTask("gen_thumbnail", "default", map[string]interface{}{"src": "some/path/to/img"})
	m4 := randomTask("sync", "default", nil)

	tests := []struct {
		enqueued   []*TaskMessage
		inProgress []*TaskMessage
		scheduled  []*TaskMessage
		retry      []*TaskMessage
		dead       []*TaskMessage
		want       *Stats
	}{
		{
			enqueued:   []*TaskMessage{m1},
			inProgress: []*TaskMessage{m2},
			scheduled:  []*TaskMessage{m3, m4},
			retry:      []*TaskMessage{},
			dead:       []*TaskMessage{},
			want: &Stats{
				Enqueued:   1,
				InProgress: 1,
				Scheduled:  2,
				Retry:      0,
				Dead:       0,
				Timestamp:  time.Now(),
			},
		},
		{
			enqueued:   []*TaskMessage{},
			inProgress: []*TaskMessage{},
			scheduled:  []*TaskMessage{m3, m4},
			retry:      []*TaskMessage{m1},
			dead:       []*TaskMessage{m2},
			want: &Stats{
				Enqueued:   0,
				InProgress: 0,
				Scheduled:  2,
				Retry:      1,
				Dead:       1,
				Timestamp:  time.Now(),
			},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		// initialize the queues.
		for _, msg := range tc.enqueued {
			if err := r.Enqueue(msg); err != nil {
				t.Fatal(err)
			}
		}
		for _, msg := range tc.inProgress {
			if err := r.client.LPush(inProgressQ, mustMarshal(t, msg)).Err(); err != nil {
				t.Fatal(err)
			}
		}
		for _, msg := range tc.scheduled {
			if err := r.Schedule(msg, time.Now().Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
		}
		for _, msg := range tc.retry {
			if err := r.RetryLater(msg, time.Now().Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
		}
		for _, msg := range tc.dead {
			if err := r.Kill(msg); err != nil {
				t.Fatal(err)
			}
		}

		got, err := r.CurrentStats()
		if err != nil {
			t.Errorf("r.CurrentStats() = %v, %v, want %v, nil", got, err, tc.want)
			continue
		}

		if diff := cmp.Diff(tc.want, got, timeCmpOpt); diff != "" {
			t.Errorf("r.CurrentStats() = %v, %v, want %v, nil; (-want, +got)\n%s", got, err, tc.want, diff)
			continue
		}
	}

}

func TestListEnqueued(t *testing.T) {
	r := setup(t)

	m1 := randomTask("send_email", "default", map[string]interface{}{"subject": "hello"})
	m2 := randomTask("reindex", "default", nil)
	t1 := &EnqueuedTask{ID: m1.ID, Type: m1.Type, Payload: m1.Payload}
	t2 := &EnqueuedTask{ID: m2.ID, Type: m2.Type, Payload: m2.Payload}
	tests := []struct {
		enqueued []*TaskMessage
		want     []*EnqueuedTask
	}{
		{
			enqueued: []*TaskMessage{m1, m2},
			want:     []*EnqueuedTask{t1, t2},
		},
		{
			enqueued: []*TaskMessage{},
			want:     []*EnqueuedTask{},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		// initialize the list
		for _, msg := range tc.enqueued {
			if err := r.Enqueue(msg); err != nil {
				t.Fatal(err)
			}
		}
		got, err := r.ListEnqueued()
		if err != nil {
			t.Errorf("r.ListEnqueued() = %v, %v, want %v, nil", got, err, tc.want)
			continue
		}
		sortOpt := cmp.Transformer("SortMsg", func(in []*EnqueuedTask) []*EnqueuedTask {
			out := append([]*EnqueuedTask(nil), in...) // Copy input to avoid mutating it
			sort.Slice(out, func(i, j int) bool {
				return out[i].ID.String() < out[j].ID.String()
			})
			return out
		})
		if diff := cmp.Diff(tc.want, got, sortOpt); diff != "" {
			t.Errorf("r.ListEnqueued() = %v, %v, want %v, nil; (-want, +got)\n%s", got, err, tc.want, diff)
			continue
		}
	}
}

func TestListInProgress(t *testing.T) {
	r := setup(t)

	m1 := randomTask("send_email", "default", map[string]interface{}{"subject": "hello"})
	m2 := randomTask("reindex", "default", nil)
	t1 := &InProgressTask{ID: m1.ID, Type: m1.Type, Payload: m1.Payload}
	t2 := &InProgressTask{ID: m2.ID, Type: m2.Type, Payload: m2.Payload}
	tests := []struct {
		enqueued []*TaskMessage
		want     []*InProgressTask
	}{
		{
			enqueued: []*TaskMessage{m1, m2},
			want:     []*InProgressTask{t1, t2},
		},
		{
			enqueued: []*TaskMessage{},
			want:     []*InProgressTask{},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		// initialize the list
		for _, msg := range tc.enqueued {
			if err := r.client.LPush(inProgressQ, mustMarshal(t, msg)).Err(); err != nil {
				t.Fatal(err)
			}
		}
		got, err := r.ListInProgress()
		if err != nil {
			t.Errorf("r.ListInProgress() = %v, %v, want %v, nil", got, err, tc.want)
			continue
		}
		sortOpt := cmp.Transformer("SortMsg", func(in []*InProgressTask) []*InProgressTask {
			out := append([]*InProgressTask(nil), in...) // Copy input to avoid mutating it
			sort.Slice(out, func(i, j int) bool {
				return out[i].ID.String() < out[j].ID.String()
			})
			return out
		})
		if diff := cmp.Diff(tc.want, got, sortOpt); diff != "" {
			t.Errorf("r.ListInProgress() = %v, %v, want %v, nil; (-want, +got)\n%s", got, err, tc.want, diff)
			continue
		}
	}
}

func TestListScheduled(t *testing.T) {
	r := setup(t)
	m1 := randomTask("send_email", "default", map[string]interface{}{"subject": "hello"})
	m2 := randomTask("reindex", "default", nil)
	p1 := time.Now().Add(30 * time.Minute)
	p2 := time.Now().Add(24 * time.Hour)
	t1 := &ScheduledTask{ID: m1.ID, Type: m1.Type, Payload: m1.Payload, ProcessAt: p1, Score: p1.Unix()}
	t2 := &ScheduledTask{ID: m2.ID, Type: m2.Type, Payload: m2.Payload, ProcessAt: p2, Score: p2.Unix()}

	type scheduledEntry struct {
		msg       *TaskMessage
		processAt time.Time
	}

	tests := []struct {
		scheduled []scheduledEntry
		want      []*ScheduledTask
	}{
		{
			scheduled: []scheduledEntry{
				{m1, p1},
				{m2, p2},
			},
			want: []*ScheduledTask{t1, t2},
		},
		{
			scheduled: []scheduledEntry{},
			want:      []*ScheduledTask{},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		// initialize the scheduled queue
		for _, s := range tc.scheduled {
			err := r.Schedule(s.msg, s.processAt)
			if err != nil {
				t.Fatal(err)
			}
		}

		got, err := r.ListScheduled()
		if err != nil {
			t.Errorf("r.ListScheduled() = %v, %v, want %v, nil", got, err, tc.want)
			continue
		}
		sortOpt := cmp.Transformer("SortMsg", func(in []*ScheduledTask) []*ScheduledTask {
			out := append([]*ScheduledTask(nil), in...) // Copy input to avoid mutating it
			sort.Slice(out, func(i, j int) bool {
				return out[i].ID.String() < out[j].ID.String()
			})
			return out
		})

		if diff := cmp.Diff(tc.want, got, sortOpt, timeCmpOpt); diff != "" {
			t.Errorf("r.ListScheduled() = %v, %v, want %v, nil; (-want, +got)\n%s", got, err, tc.want, diff)
			continue
		}
	}
}

func TestListRetry(t *testing.T) {
	r := setup(t)
	m1 := &TaskMessage{
		ID:       xid.New(),
		Type:     "send_email",
		Queue:    "default",
		Payload:  map[string]interface{}{"subject": "hello"},
		ErrorMsg: "email server not responding",
		Retry:    25,
		Retried:  10,
	}
	m2 := &TaskMessage{
		ID:       xid.New(),
		Type:     "reindex",
		Queue:    "default",
		Payload:  nil,
		ErrorMsg: "search engine not responding",
		Retry:    25,
		Retried:  2,
	}
	p1 := time.Now().Add(5 * time.Minute)
	p2 := time.Now().Add(24 * time.Hour)
	t1 := &RetryTask{ID: m1.ID, Type: m1.Type, Payload: m1.Payload,
		ProcessAt: p1, ErrorMsg: m1.ErrorMsg, Retried: m1.Retried,
		Retry: m1.Retry, Score: p1.Unix()}
	t2 := &RetryTask{ID: m2.ID, Type: m2.Type, Payload: m2.Payload,
		ProcessAt: p2, ErrorMsg: m2.ErrorMsg, Retried: m2.Retried,
		Retry: m2.Retry, Score: p2.Unix()}

	type retryEntry struct {
		msg       *TaskMessage
		processAt time.Time
	}

	tests := []struct {
		dead []retryEntry
		want []*RetryTask
	}{
		{
			dead: []retryEntry{
				{m1, p1},
				{m2, p2},
			},
			want: []*RetryTask{t1, t2},
		},
		{
			dead: []retryEntry{},
			want: []*RetryTask{},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		// initialize the scheduled queue
		for _, d := range tc.dead {
			r.client.ZAdd(retryQ, &redis.Z{
				Member: mustMarshal(t, d.msg),
				Score:  float64(d.processAt.Unix()),
			})
		}

		got, err := r.ListRetry()
		if err != nil {
			t.Errorf("r.ListRetry() = %v, %v, want %v, nil", got, err, tc.want)
			continue
		}
		sortOpt := cmp.Transformer("SortMsg", func(in []*RetryTask) []*RetryTask {
			out := append([]*RetryTask(nil), in...) // Copy input to avoid mutating it
			sort.Slice(out, func(i, j int) bool {
				return out[i].ID.String() < out[j].ID.String()
			})
			return out
		})

		if diff := cmp.Diff(tc.want, got, sortOpt, timeCmpOpt); diff != "" {
			t.Errorf("r.ListRetry() = %v, %v, want %v, nil; (-want, +got)\n%s", got, err, tc.want, diff)
			continue
		}
	}
}

func TestListDead(t *testing.T) {
	r := setup(t)
	m1 := &TaskMessage{
		ID:       xid.New(),
		Type:     "send_email",
		Queue:    "default",
		Payload:  map[string]interface{}{"subject": "hello"},
		ErrorMsg: "email server not responding",
	}
	m2 := &TaskMessage{
		ID:       xid.New(),
		Type:     "reindex",
		Queue:    "default",
		Payload:  nil,
		ErrorMsg: "search engine not responding",
	}
	f1 := time.Now().Add(-5 * time.Minute)
	f2 := time.Now().Add(-24 * time.Hour)
	t1 := &DeadTask{ID: m1.ID, Type: m1.Type, Payload: m1.Payload,
		LastFailedAt: f1, ErrorMsg: m1.ErrorMsg, Score: f1.Unix()}
	t2 := &DeadTask{ID: m2.ID, Type: m2.Type, Payload: m2.Payload,
		LastFailedAt: f2, ErrorMsg: m2.ErrorMsg, Score: f2.Unix()}

	type deadEntry struct {
		msg          *TaskMessage
		lastFailedAt time.Time
	}

	tests := []struct {
		dead []deadEntry
		want []*DeadTask
	}{
		{
			dead: []deadEntry{
				{m1, f1},
				{m2, f2},
			},
			want: []*DeadTask{t1, t2},
		},
		{
			dead: []deadEntry{},
			want: []*DeadTask{},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		// initialize the scheduled queue
		for _, d := range tc.dead {
			r.client.ZAdd(deadQ, &redis.Z{
				Member: mustMarshal(t, d.msg),
				Score:  float64(d.lastFailedAt.Unix()),
			})
		}

		got, err := r.ListDead()
		if err != nil {
			t.Errorf("r.ListDead() = %v, %v, want %v, nil", got, err, tc.want)
			continue
		}
		sortOpt := cmp.Transformer("SortMsg", func(in []*DeadTask) []*DeadTask {
			out := append([]*DeadTask(nil), in...) // Copy input to avoid mutating it
			sort.Slice(out, func(i, j int) bool {
				return out[i].ID.String() < out[j].ID.String()
			})
			return out
		})

		if diff := cmp.Diff(tc.want, got, sortOpt, timeCmpOpt); diff != "" {
			t.Errorf("r.ListDead() = %v, %v, want %v, nil; (-want, +got)\n%s", got, err, tc.want, diff)
			continue
		}
	}
}

var timeCmpOpt = EquateApproxTime(time.Second)

func TestEnqueueDeadTask(t *testing.T) {
	r := setup(t)

	t1 := randomTask("send_email", "default", nil)
	t2 := randomTask("gen_thumbnail", "default", nil)
	s1 := time.Now().Add(-5 * time.Minute).Unix()
	s2 := time.Now().Add(-time.Hour).Unix()
	type deadEntry struct {
		msg   *TaskMessage
		score int64
	}
	tests := []struct {
		dead         []deadEntry
		score        int64
		id           xid.ID
		want         error // expected return value from calling EnqueueDeadTask
		wantDead     []*TaskMessage
		wantEnqueued []*TaskMessage
	}{
		{
			dead: []deadEntry{
				{t1, s1},
				{t2, s2},
			},
			score:        s2,
			id:           t2.ID,
			want:         nil,
			wantDead:     []*TaskMessage{t1},
			wantEnqueued: []*TaskMessage{t2},
		},
		{
			dead: []deadEntry{
				{t1, s1},
				{t2, s2},
			},
			score:        123,
			id:           t2.ID,
			want:         ErrTaskNotFound,
			wantDead:     []*TaskMessage{t1, t2},
			wantEnqueued: []*TaskMessage{},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		// initialize dead queue
		for _, d := range tc.dead {
			err := r.client.ZAdd(deadQ, &redis.Z{Member: mustMarshal(t, d.msg), Score: float64(d.score)}).Err()
			if err != nil {
				t.Fatal(err)
			}
		}

		got := r.EnqueueDeadTask(tc.id, tc.score)
		if got != tc.want {
			t.Errorf("r.EnqueueDeadTask(%s, %d) = %v, want %v", tc.id, tc.score, got, tc.want)
			continue
		}

		gotEnqueuedRaw := r.client.LRange(defaultQ, 0, -1).Val()
		gotEnqueued := mustUnmarshalSlice(t, gotEnqueuedRaw)
		if diff := cmp.Diff(tc.wantEnqueued, gotEnqueued, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", defaultQ, diff)
		}

		gotDeadRaw := r.client.ZRange(deadQ, 0, -1).Val()
		gotDead := mustUnmarshalSlice(t, gotDeadRaw)
		if diff := cmp.Diff(tc.wantDead, gotDead, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q, (-want, +got)\n%s", deadQ, diff)
		}
	}
}

func TestEnqueueRetryTask(t *testing.T) {
	r := setup(t)

	t1 := randomTask("send_email", "default", nil)
	t2 := randomTask("gen_thumbnail", "default", nil)
	s1 := time.Now().Add(-5 * time.Minute).Unix()
	s2 := time.Now().Add(-time.Hour).Unix()
	type retryEntry struct {
		msg   *TaskMessage
		score int64
	}
	tests := []struct {
		dead         []retryEntry
		score        int64
		id           xid.ID
		want         error // expected return value from calling EnqueueRetryTask
		wantRetry    []*TaskMessage
		wantEnqueued []*TaskMessage
	}{
		{
			dead: []retryEntry{
				{t1, s1},
				{t2, s2},
			},
			score:        s2,
			id:           t2.ID,
			want:         nil,
			wantRetry:    []*TaskMessage{t1},
			wantEnqueued: []*TaskMessage{t2},
		},
		{
			dead: []retryEntry{
				{t1, s1},
				{t2, s2},
			},
			score:        123,
			id:           t2.ID,
			want:         ErrTaskNotFound,
			wantRetry:    []*TaskMessage{t1, t2},
			wantEnqueued: []*TaskMessage{},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		// initialize retry queue
		for _, d := range tc.dead {
			err := r.client.ZAdd(retryQ, &redis.Z{Member: mustMarshal(t, d.msg), Score: float64(d.score)}).Err()
			if err != nil {
				t.Fatal(err)
			}
		}

		got := r.EnqueueRetryTask(tc.id, tc.score)
		if got != tc.want {
			t.Errorf("r.EnqueueRetryTask(%s, %d) = %v, want %v", tc.id, tc.score, got, tc.want)
			continue
		}

		gotEnqueuedRaw := r.client.LRange(defaultQ, 0, -1).Val()
		gotEnqueued := mustUnmarshalSlice(t, gotEnqueuedRaw)
		if diff := cmp.Diff(tc.wantEnqueued, gotEnqueued, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", defaultQ, diff)
		}

		gotRetryRaw := r.client.ZRange(retryQ, 0, -1).Val()
		gotRetry := mustUnmarshalSlice(t, gotRetryRaw)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q, (-want, +got)\n%s", retryQ, diff)
		}
	}
}

func TestEnqueueScheduledTask(t *testing.T) {
	r := setup(t)

	t1 := randomTask("send_email", "default", nil)
	t2 := randomTask("gen_thumbnail", "default", nil)
	s1 := time.Now().Add(-5 * time.Minute).Unix()
	s2 := time.Now().Add(-time.Hour).Unix()
	type scheduledEntry struct {
		msg   *TaskMessage
		score int64
	}
	tests := []struct {
		dead          []scheduledEntry
		score         int64
		id            xid.ID
		want          error // expected return value from calling EnqueueScheduledTask
		wantScheduled []*TaskMessage
		wantEnqueued  []*TaskMessage
	}{
		{
			dead: []scheduledEntry{
				{t1, s1},
				{t2, s2},
			},
			score:         s2,
			id:            t2.ID,
			want:          nil,
			wantScheduled: []*TaskMessage{t1},
			wantEnqueued:  []*TaskMessage{t2},
		},
		{
			dead: []scheduledEntry{
				{t1, s1},
				{t2, s2},
			},
			score:         123,
			id:            t2.ID,
			want:          ErrTaskNotFound,
			wantScheduled: []*TaskMessage{t1, t2},
			wantEnqueued:  []*TaskMessage{},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		// initialize scheduled queue
		for _, d := range tc.dead {
			err := r.client.ZAdd(scheduledQ, &redis.Z{Member: mustMarshal(t, d.msg), Score: float64(d.score)}).Err()
			if err != nil {
				t.Fatal(err)
			}
		}

		got := r.EnqueueScheduledTask(tc.id, tc.score)
		if got != tc.want {
			t.Errorf("r.EnqueueRetryTask(%s, %d) = %v, want %v", tc.id, tc.score, got, tc.want)
			continue
		}

		gotEnqueuedRaw := r.client.LRange(defaultQ, 0, -1).Val()
		gotEnqueued := mustUnmarshalSlice(t, gotEnqueuedRaw)
		if diff := cmp.Diff(tc.wantEnqueued, gotEnqueued, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", defaultQ, diff)
		}

		gotScheduledRaw := r.client.ZRange(scheduledQ, 0, -1).Val()
		gotScheduled := mustUnmarshalSlice(t, gotScheduledRaw)
		if diff := cmp.Diff(tc.wantScheduled, gotScheduled, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q, (-want, +got)\n%s", scheduledQ, diff)
		}
	}
}

func TestEnqueueAllScheduledTasks(t *testing.T) {
	r := setup(t)
	t1 := randomTask("send_email", "default", nil)
	t2 := randomTask("gen_thumbnail", "default", nil)
	t3 := randomTask("reindex", "default", nil)

	tests := []struct {
		description  string
		scheduled    []*TaskMessage
		want         int64
		wantEnqueued []*TaskMessage
	}{
		{
			description:  "with tasks in scheduled queue",
			scheduled:    []*TaskMessage{t1, t2, t3},
			want:         3,
			wantEnqueued: []*TaskMessage{t1, t2, t3},
		},
		{
			description:  "with empty scheduled queue",
			scheduled:    []*TaskMessage{},
			want:         0,
			wantEnqueued: []*TaskMessage{},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		// initialize scheduled queue
		for _, msg := range tc.scheduled {
			err := r.client.ZAdd(scheduledQ, &redis.Z{
				Member: mustMarshal(t, msg),
				Score:  float64(time.Now().Add(time.Hour).Unix())}).Err()
			if err != nil {
				t.Fatal(err)
			}
		}

		got, err := r.EnqueueAllScheduledTasks()
		if err != nil {
			t.Errorf("%s; r.EnqueueAllScheduledTasks = %v, %v; want %v, nil",
				tc.description, got, err, tc.want)
			continue
		}

		if got != tc.want {
			t.Errorf("%s; r.EnqueueAllScheduledTasks = %v, %v; want %v, nil",
				tc.description, got, err, tc.want)
		}

		gotEnqueuedRaw := r.client.LRange(defaultQ, 0, -1).Val()
		gotEnqueued := mustUnmarshalSlice(t, gotEnqueuedRaw)
		if diff := cmp.Diff(tc.wantEnqueued, gotEnqueued, sortMsgOpt); diff != "" {
			t.Errorf("%s; mismatch found in %q; (-want, +got)\n%s", tc.description, defaultQ, diff)
		}
	}
}

func TestEnqueueAllRetryTasks(t *testing.T) {
	r := setup(t)
	t1 := randomTask("send_email", "default", nil)
	t2 := randomTask("gen_thumbnail", "default", nil)
	t3 := randomTask("reindex", "default", nil)

	tests := []struct {
		description  string
		retry        []*TaskMessage
		want         int64
		wantEnqueued []*TaskMessage
	}{
		{
			description:  "with tasks in retry queue",
			retry:        []*TaskMessage{t1, t2, t3},
			want:         3,
			wantEnqueued: []*TaskMessage{t1, t2, t3},
		},
		{
			description:  "with empty retry queue",
			retry:        []*TaskMessage{},
			want:         0,
			wantEnqueued: []*TaskMessage{},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		// initialize retry queue
		for _, msg := range tc.retry {
			err := r.client.ZAdd(retryQ, &redis.Z{
				Member: mustMarshal(t, msg),
				Score:  float64(time.Now().Add(time.Hour).Unix())}).Err()
			if err != nil {
				t.Fatal(err)
			}
		}

		got, err := r.EnqueueAllRetryTasks()
		if err != nil {
			t.Errorf("%s; r.EnqueueAllRetryTasks = %v, %v; want %v, nil",
				tc.description, got, err, tc.want)
			continue
		}

		if got != tc.want {
			t.Errorf("%s; r.EnqueueAllRetryTasks = %v, %v; want %v, nil",
				tc.description, got, err, tc.want)
		}

		gotEnqueuedRaw := r.client.LRange(defaultQ, 0, -1).Val()
		gotEnqueued := mustUnmarshalSlice(t, gotEnqueuedRaw)
		if diff := cmp.Diff(tc.wantEnqueued, gotEnqueued, sortMsgOpt); diff != "" {
			t.Errorf("%s; mismatch found in %q; (-want, +got)\n%s", tc.description, defaultQ, diff)
		}
	}
}

func TestEnqueueAllDeadTasks(t *testing.T) {
	r := setup(t)
	t1 := randomTask("send_email", "default", nil)
	t2 := randomTask("gen_thumbnail", "default", nil)
	t3 := randomTask("reindex", "default", nil)

	tests := []struct {
		description  string
		dead         []*TaskMessage
		want         int64
		wantEnqueued []*TaskMessage
	}{
		{
			description:  "with tasks in dead queue",
			dead:         []*TaskMessage{t1, t2, t3},
			want:         3,
			wantEnqueued: []*TaskMessage{t1, t2, t3},
		},
		{
			description:  "with empty dead queue",
			dead:         []*TaskMessage{},
			want:         0,
			wantEnqueued: []*TaskMessage{},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		// initialize dead queue
		for _, msg := range tc.dead {
			err := r.client.ZAdd(deadQ, &redis.Z{
				Member: mustMarshal(t, msg),
				Score:  float64(time.Now().Add(time.Hour).Unix())}).Err()
			if err != nil {
				t.Fatal(err)
			}
		}

		got, err := r.EnqueueAllDeadTasks()
		if err != nil {
			t.Errorf("%s; r.EnqueueAllDeadTasks = %v, %v; want %v, nil",
				tc.description, got, err, tc.want)
			continue
		}

		if got != tc.want {
			t.Errorf("%s; r.EnqueueAllDeadTasks = %v, %v; want %v, nil",
				tc.description, got, err, tc.want)
		}

		gotEnqueuedRaw := r.client.LRange(defaultQ, 0, -1).Val()
		gotEnqueued := mustUnmarshalSlice(t, gotEnqueuedRaw)
		if diff := cmp.Diff(tc.wantEnqueued, gotEnqueued, sortMsgOpt); diff != "" {
			t.Errorf("%s; mismatch found in %q; (-want, +got)\n%s", tc.description, defaultQ, diff)
		}
	}
}

func TestDeleteDeadTask(t *testing.T) {
	r := setup(t)
	m1 := randomTask("send_email", "default", nil)
	m2 := randomTask("reindex", "default", nil)
	t1 := time.Now().Add(-5 * time.Minute)
	t2 := time.Now().Add(-time.Hour)

	type deadEntry struct {
		msg   *TaskMessage
		score int64
	}
	tests := []struct {
		dead     []deadEntry
		id       xid.ID
		score    int64
		want     error
		wantDead []*TaskMessage
	}{
		{
			dead: []deadEntry{
				{m1, t1.Unix()},
				{m2, t2.Unix()},
			},
			id:       m1.ID,
			score:    t1.Unix(),
			want:     nil,
			wantDead: []*TaskMessage{m2},
		},
		{
			dead: []deadEntry{
				{m1, t1.Unix()},
				{m2, t2.Unix()},
			},
			id:       m1.ID,
			score:    t2.Unix(), // id and score mismatch
			want:     ErrTaskNotFound,
			wantDead: []*TaskMessage{m1, m2},
		},
		{
			dead:     []deadEntry{},
			id:       m1.ID,
			score:    t1.Unix(),
			want:     ErrTaskNotFound,
			wantDead: []*TaskMessage{},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		// initialize dead queue.
		for _, d := range tc.dead {
			err := r.client.ZAdd(deadQ, &redis.Z{
				Member: mustMarshal(t, d.msg),
				Score:  float64(d.score),
			}).Err()
			if err != nil {
				t.Fatal(err)
			}
		}

		got := r.DeleteDeadTask(tc.id, tc.score)
		if got != tc.want {
			t.Errorf("r.DeleteDeadTask(%v, %v) = %v, want %v", tc.id, tc.score, got, tc.want)
			continue
		}

		gotDeadRaw := r.client.ZRange(deadQ, 0, -1).Val()
		gotDead := mustUnmarshalSlice(t, gotDeadRaw)
		if diff := cmp.Diff(tc.wantDead, gotDead, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", deadQ, diff)
		}
	}
}

func TestDeleteRetryTask(t *testing.T) {
	r := setup(t)
	m1 := randomTask("send_email", "default", nil)
	m2 := randomTask("reindex", "default", nil)
	t1 := time.Now().Add(5 * time.Minute)
	t2 := time.Now().Add(time.Hour)

	type retryEntry struct {
		msg   *TaskMessage
		score int64
	}
	tests := []struct {
		retry     []retryEntry
		id        xid.ID
		score     int64
		want      error
		wantRetry []*TaskMessage
	}{
		{
			retry: []retryEntry{
				{m1, t1.Unix()},
				{m2, t2.Unix()},
			},
			id:        m1.ID,
			score:     t1.Unix(),
			want:      nil,
			wantRetry: []*TaskMessage{m2},
		},
		{
			retry: []retryEntry{
				{m1, t1.Unix()},
			},
			id:        m2.ID,
			score:     t2.Unix(),
			want:      ErrTaskNotFound,
			wantRetry: []*TaskMessage{m1},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		// initialize retry queue.
		for _, e := range tc.retry {
			err := r.client.ZAdd(retryQ, &redis.Z{
				Member: mustMarshal(t, e.msg),
				Score:  float64(e.score),
			}).Err()
			if err != nil {
				t.Fatal(err)
			}
		}

		got := r.DeleteRetryTask(tc.id, tc.score)
		if got != tc.want {
			t.Errorf("r.DeleteRetryTask(%v, %v) = %v, want %v", tc.id, tc.score, got, tc.want)
			continue
		}

		gotRetryRaw := r.client.ZRange(retryQ, 0, -1).Val()
		gotRetry := mustUnmarshalSlice(t, gotRetryRaw)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", retryQ, diff)
		}
	}
}

func TestDeleteScheduledTask(t *testing.T) {
	r := setup(t)
	m1 := randomTask("send_email", "default", nil)
	m2 := randomTask("reindex", "default", nil)
	t1 := time.Now().Add(5 * time.Minute)
	t2 := time.Now().Add(time.Hour)

	type scheduledEntry struct {
		msg   *TaskMessage
		score int64
	}
	tests := []struct {
		scheduled     []scheduledEntry
		id            xid.ID
		score         int64
		want          error
		wantScheduled []*TaskMessage
	}{
		{
			scheduled: []scheduledEntry{
				{m1, t1.Unix()},
				{m2, t2.Unix()},
			},
			id:            m1.ID,
			score:         t1.Unix(),
			want:          nil,
			wantScheduled: []*TaskMessage{m2},
		},
		{
			scheduled: []scheduledEntry{
				{m1, t1.Unix()},
			},
			id:            m2.ID,
			score:         t2.Unix(),
			want:          ErrTaskNotFound,
			wantScheduled: []*TaskMessage{m1},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		// initialize scheduled queue.
		for _, e := range tc.scheduled {
			err := r.client.ZAdd(scheduledQ, &redis.Z{
				Member: mustMarshal(t, e.msg),
				Score:  float64(e.score),
			}).Err()
			if err != nil {
				t.Fatal(err)
			}
		}

		got := r.DeleteScheduledTask(tc.id, tc.score)
		if got != tc.want {
			t.Errorf("r.DeleteScheduledTask(%v, %v) = %v, want %v", tc.id, tc.score, got, tc.want)
			continue
		}

		gotScheduledRaw := r.client.ZRange(scheduledQ, 0, -1).Val()
		gotScheduled := mustUnmarshalSlice(t, gotScheduledRaw)
		if diff := cmp.Diff(tc.wantScheduled, gotScheduled, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", scheduledQ, diff)
		}
	}
}

func TestDeleteAllDeadTasks(t *testing.T) {
	r := setup(t)
	m1 := randomTask("send_email", "default", nil)
	m2 := randomTask("reindex", "default", nil)
	m3 := randomTask("gen_thumbnail", "default", nil)

	tests := []struct {
		initDead []*TaskMessage
		wantDead []*TaskMessage
	}{
		{
			initDead: []*TaskMessage{m1, m2, m3},
			wantDead: []*TaskMessage{},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		// initialize dead queue.
		for _, msg := range tc.initDead {
			err := r.client.ZAdd(deadQ, &redis.Z{
				Member: mustMarshal(t, msg),
				Score:  float64(time.Now().Unix()),
			}).Err()
			if err != nil {
				t.Fatal(err)
			}
		}

		err := r.DeleteAllDeadTasks()
		if err != nil {
			t.Errorf("r.DeleteAllDeaadTasks = %v, want nil", err)
		}

		gotDeadRaw := r.client.ZRange(deadQ, 0, -1).Val()
		gotDead := mustUnmarshalSlice(t, gotDeadRaw)
		if diff := cmp.Diff(tc.wantDead, gotDead, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", deadQ, diff)
		}
	}
}

func TestDeleteAllRetryTasks(t *testing.T) {
	r := setup(t)
	m1 := randomTask("send_email", "default", nil)
	m2 := randomTask("reindex", "default", nil)
	m3 := randomTask("gen_thumbnail", "default", nil)

	tests := []struct {
		initRetry []*TaskMessage
		wantRetry []*TaskMessage
	}{
		{
			initRetry: []*TaskMessage{m1, m2, m3},
			wantRetry: []*TaskMessage{},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		// initialize retry queue.
		for _, msg := range tc.initRetry {
			err := r.client.ZAdd(retryQ, &redis.Z{
				Member: mustMarshal(t, msg),
				Score:  float64(time.Now().Unix()),
			}).Err()
			if err != nil {
				t.Fatal(err)
			}
		}

		err := r.DeleteAllRetryTasks()
		if err != nil {
			t.Errorf("r.DeleteAllDeaadTasks = %v, want nil", err)
		}

		gotRetryRaw := r.client.ZRange(retryQ, 0, -1).Val()
		gotRetry := mustUnmarshalSlice(t, gotRetryRaw)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", retryQ, diff)
		}
	}
}

func TestDeleteAllScheduledTasks(t *testing.T) {
	r := setup(t)
	m1 := randomTask("send_email", "default", nil)
	m2 := randomTask("reindex", "default", nil)
	m3 := randomTask("gen_thumbnail", "default", nil)

	tests := []struct {
		initScheduled []*TaskMessage
		wantScheduled []*TaskMessage
	}{
		{
			initScheduled: []*TaskMessage{m1, m2, m3},
			wantScheduled: []*TaskMessage{},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		// initialize scheduled queue.
		for _, msg := range tc.initScheduled {
			err := r.client.ZAdd(scheduledQ, &redis.Z{
				Member: mustMarshal(t, msg),
				Score:  float64(time.Now().Unix()),
			}).Err()
			if err != nil {
				t.Fatal(err)
			}
		}

		err := r.DeleteAllScheduledTasks()
		if err != nil {
			t.Errorf("r.DeleteAllDeaadTasks = %v, want nil", err)
		}

		gotScheduledRaw := r.client.ZRange(scheduledQ, 0, -1).Val()
		gotScheduled := mustUnmarshalSlice(t, gotScheduledRaw)
		if diff := cmp.Diff(tc.wantScheduled, gotScheduled, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", scheduledQ, diff)
		}
	}
}
