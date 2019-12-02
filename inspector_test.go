package asynq

import (
	"sort"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
)

// ---- TODO(hibiken): Remove this once the new version is released (https://github.com/google/go-cmp/issues/166) ----
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

//-----------------------------

func TestCurrentStats(t *testing.T) {
	r := setup(t)
	inspector := &Inspector{r}
	t1 := randomTask("send_email", "default", nil)
	t2 := randomTask("send_email", "default", nil)
	t3 := randomTask("gen_export", "default", nil)
	t4 := randomTask("gen_thumbnail", "default", nil)
	t5 := randomTask("send_email", "default", nil)

	tests := []struct {
		queue      []*taskMessage
		inProgress []*taskMessage
		scheduled  []*taskMessage
		retry      []*taskMessage
		dead       []*taskMessage
		want       *Stats
	}{
		{
			queue:      []*taskMessage{t1},
			inProgress: []*taskMessage{t2, t3},
			scheduled:  []*taskMessage{t4},
			retry:      []*taskMessage{},
			dead:       []*taskMessage{t5},
			want: &Stats{
				Queued:     1,
				InProgress: 2,
				Scheduled:  1,
				Retry:      0,
				Dead:       1,
			},
		},
		{
			queue:      []*taskMessage{},
			inProgress: []*taskMessage{},
			scheduled:  []*taskMessage{t1, t2, t4},
			retry:      []*taskMessage{t3},
			dead:       []*taskMessage{t5},
			want: &Stats{
				Queued:     0,
				InProgress: 0,
				Scheduled:  3,
				Retry:      1,
				Dead:       1,
			},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		for _, msg := range tc.queue {
			err := r.client.LPush(defaultQueue, mustMarshal(t, msg)).Err()
			if err != nil {
				t.Fatal(err)
			}
		}
		for _, msg := range tc.inProgress {
			err := r.client.LPush(inProgress, mustMarshal(t, msg)).Err()
			if err != nil {
				t.Fatal(err)
			}
		}
		for _, msg := range tc.scheduled {
			err := r.client.ZAdd(scheduled, &redis.Z{
				Member: mustMarshal(t, msg),
				Score:  float64(time.Now().Add(time.Hour).Unix()),
			}).Err()
			if err != nil {
				t.Fatal(err)
			}
		}
		for _, msg := range tc.retry {
			err := r.client.ZAdd(retry, &redis.Z{
				Member: mustMarshal(t, msg),
				Score:  float64(time.Now().Add(time.Hour).Unix()),
			}).Err()
			if err != nil {
				t.Fatal(err)
			}
		}
		for _, msg := range tc.dead {
			err := r.client.ZAdd(dead, &redis.Z{
				Member: mustMarshal(t, msg),
				Score:  float64(time.Now().Unix()),
			}).Err()
			if err != nil {
				t.Fatal(err)
			}
		}

		got, err := inspector.CurrentStats()
		if err != nil {
			t.Error(err)
			continue
		}
		ignoreOpt := cmpopts.IgnoreFields(*tc.want, "Timestamp")
		if diff := cmp.Diff(tc.want, got, ignoreOpt); diff != "" {
			t.Errorf("(*Inspector).CurrentStats() = %+v, want %+v; (-want, +got)\n%s",
				got, tc.want, diff)
			continue
		}
	}
}

func TestListEnqueuedTasks(t *testing.T) {
	r := setup(t)
	inspector := &Inspector{r}
	m1 := randomTask("send_email", "default", nil)
	m2 := randomTask("send_email", "default", nil)
	m3 := randomTask("gen_export", "default", nil)
	t1 := &EnqueuedTask{ID: m1.ID, Type: m1.Type, Payload: m1.Payload}
	t2 := &EnqueuedTask{ID: m2.ID, Type: m2.Type, Payload: m2.Payload}
	t3 := &EnqueuedTask{ID: m3.ID, Type: m3.Type, Payload: m3.Payload}

	tests := []struct {
		queued []*taskMessage
		want   []*EnqueuedTask
	}{
		{
			queued: []*taskMessage{m1, m2, m3},
			want:   []*EnqueuedTask{t1, t2, t3},
		},
		{
			queued: []*taskMessage{},
			want:   []*EnqueuedTask{},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		for _, msg := range tc.queued {
			err := r.client.LPush(defaultQueue, mustMarshal(t, msg)).Err()
			if err != nil {
				t.Fatal(err)
			}
		}

		got, err := inspector.ListEnqueuedTasks()
		if err != nil {
			t.Error(err)
			continue
		}

		sortOpt := cmp.Transformer("SortEnqueuedTasks", func(in []*EnqueuedTask) []*EnqueuedTask {
			out := append([]*EnqueuedTask(nil), in...) // Copy input to avoid mutating it
			sort.Slice(out, func(i, j int) bool {
				return out[i].ID.String() < out[j].ID.String()
			})
			return out
		})
		if diff := cmp.Diff(tc.want, got, sortOpt); diff != "" {
			t.Errorf("(*Inspector).ListEnqueuedTasks = %v, want %v; (-want, +got)\n%s",
				got, tc.want, diff)
			continue
		}
	}
}

func TestListInProgressTasks(t *testing.T) {
	r := setup(t)
	inspector := &Inspector{r}
	m1 := randomTask("send_email", "default", nil)
	m2 := randomTask("send_email", "default", nil)
	m3 := randomTask("gen_export", "default", nil)
	t1 := &InProgressTask{ID: m1.ID, Type: m1.Type, Payload: m1.Payload}
	t2 := &InProgressTask{ID: m2.ID, Type: m2.Type, Payload: m2.Payload}
	t3 := &InProgressTask{ID: m3.ID, Type: m3.Type, Payload: m3.Payload}

	tests := []struct {
		inProgress []*taskMessage
		want       []*InProgressTask
	}{
		{
			inProgress: []*taskMessage{m1, m2, m3},
			want:       []*InProgressTask{t1, t2, t3},
		},
		{
			inProgress: []*taskMessage{},
			want:       []*InProgressTask{},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		for _, msg := range tc.inProgress {
			err := r.client.LPush(inProgress, mustMarshal(t, msg)).Err()
			if err != nil {
				t.Fatal(err)
			}
		}

		got, err := inspector.ListInProgressTasks()
		if err != nil {
			t.Error(err)
			continue
		}

		sortOpt := cmp.Transformer("SortInProgressTasks", func(in []*InProgressTask) []*InProgressTask {
			out := append([]*InProgressTask(nil), in...) // Copy input to avoid mutating it
			sort.Slice(out, func(i, j int) bool {
				return out[i].ID.String() < out[j].ID.String()
			})
			return out
		})
		if diff := cmp.Diff(tc.want, got, sortOpt); diff != "" {
			t.Errorf("(*Inspector).ListInProgressTasks = %v, want %v; (-want, +got)\n%s",
				got, tc.want, diff)
			continue
		}
	}
}

func TestListScheduledTasks(t *testing.T) {
	r := setup(t)
	inspector := &Inspector{r}
	m1 := randomTask("send_email", "default", nil)
	m2 := randomTask("send_email", "default", nil)
	m3 := randomTask("gen_export", "default", nil)
	t1 := time.Now().Add(5 * time.Minute)
	t2 := time.Now().Add(time.Hour)
	t3 := time.Now().Add(24 * time.Hour)
	s1 := &ScheduledTask{ID: m1.ID, Type: m1.Type, Payload: m1.Payload, ProcessAt: t1}
	s2 := &ScheduledTask{ID: m2.ID, Type: m2.Type, Payload: m2.Payload, ProcessAt: t2}
	s3 := &ScheduledTask{ID: m3.ID, Type: m3.Type, Payload: m3.Payload, ProcessAt: t3}

	type scheduledMsg struct {
		msg       *taskMessage
		processAt time.Time
	}
	tests := []struct {
		scheduled []scheduledMsg
		want      []*ScheduledTask
	}{
		{
			scheduled: []scheduledMsg{
				{m1, t1},
				{m2, t2},
				{m3, t3},
			},
			want: []*ScheduledTask{s1, s2, s3},
		},
		{
			scheduled: []scheduledMsg{},
			want:      []*ScheduledTask{},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		for _, s := range tc.scheduled {
			err := r.client.ZAdd(scheduled, &redis.Z{Member: mustMarshal(t, s.msg), Score: float64(s.processAt.Unix())}).Err()
			if err != nil {
				t.Fatal(err)
			}
		}

		got, err := inspector.ListScheduledTasks()
		if err != nil {
			t.Error(err)
			continue
		}

		sortOpt := cmp.Transformer("SortScheduledTasks", func(in []*ScheduledTask) []*ScheduledTask {
			out := append([]*ScheduledTask(nil), in...) // Copy input to avoid mutating it
			sort.Slice(out, func(i, j int) bool {
				return out[i].ID.String() < out[j].ID.String()
			})
			return out
		})
		timeCmpOpt := EquateApproxTime(time.Second)
		if diff := cmp.Diff(tc.want, got, sortOpt, timeCmpOpt); diff != "" {
			t.Errorf("(*Inspector).ListScheduledTasks = %v, want %v; (-want, +got)\n%s",
				got, tc.want, diff)
			continue
		}
	}
}

func TestListRetryTasks(t *testing.T) {
	r := setup(t)
	inspector := &Inspector{r}
	m1 := &taskMessage{
		ID:       uuid.New(),
		Type:     "send_email",
		Payload:  map[string]interface{}{"to": "customer@example.com"},
		ErrorMsg: "couldn't send email",
		Retry:    25,
		Retried:  10,
	}
	m2 := &taskMessage{
		ID:       uuid.New(),
		Type:     "gen_thumbnail",
		Payload:  map[string]interface{}{"src": "some/path/to/img/file"},
		ErrorMsg: "couldn't find a file",
		Retry:    20,
		Retried:  3,
	}
	t1 := time.Now().Add(time.Hour)
	t2 := time.Now().Add(24 * time.Hour)
	r1 := &RetryTask{
		ID:        m1.ID,
		Type:      m1.Type,
		Payload:   m1.Payload,
		ProcessAt: t1,
		ErrorMsg:  m1.ErrorMsg,
		Retry:     m1.Retry,
		Retried:   m1.Retried,
	}
	r2 := &RetryTask{
		ID:        m2.ID,
		Type:      m2.Type,
		Payload:   m2.Payload,
		ProcessAt: t2,
		ErrorMsg:  m2.ErrorMsg,
		Retry:     m2.Retry,
		Retried:   m2.Retried,
	}

	type retryEntry struct {
		msg       *taskMessage
		processAt time.Time
	}
	tests := []struct {
		retry []retryEntry
		want  []*RetryTask
	}{
		{
			retry: []retryEntry{
				{m1, t1},
				{m2, t2},
			},
			want: []*RetryTask{r1, r2},
		},
		{
			retry: []retryEntry{},
			want:  []*RetryTask{},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		for _, e := range tc.retry {
			err := r.client.ZAdd(retry, &redis.Z{
				Member: mustMarshal(t, e.msg),
				Score:  float64(e.processAt.Unix())}).Err()
			if err != nil {
				t.Fatal(err)
			}
		}

		got, err := inspector.ListRetryTasks()
		if err != nil {
			t.Error(err)
			continue
		}

		sortOpt := cmp.Transformer("SortRetryTasks", func(in []*RetryTask) []*RetryTask {
			out := append([]*RetryTask(nil), in...) // Copy input to avoid mutating it
			sort.Slice(out, func(i, j int) bool {
				return out[i].ID.String() < out[j].ID.String()
			})
			return out
		})
		timeCmpOpt := EquateApproxTime(time.Second)
		if diff := cmp.Diff(tc.want, got, sortOpt, timeCmpOpt); diff != "" {
			t.Errorf("(*Inspector).ListRetryTasks = %v, want %v; (-want, +got)\n%s",
				got, tc.want, diff)
			continue
		}
	}
}

func TestListDeadTasks(t *testing.T) {
	r := setup(t)
	inspector := &Inspector{r}
	m1 := &taskMessage{ID: uuid.New(), Type: "send_email", Payload: map[string]interface{}{"to": "customer@example.com"}, ErrorMsg: "couldn't send email"}
	m2 := &taskMessage{ID: uuid.New(), Type: "gen_thumbnail", Payload: map[string]interface{}{"src": "path/to/img/file"}, ErrorMsg: "couldn't find file"}
	t1 := time.Now().Add(-5 * time.Second)
	t2 := time.Now().Add(-24 * time.Hour)
	d1 := &DeadTask{ID: m1.ID, Type: m1.Type, Payload: m1.Payload, ErrorMsg: m1.ErrorMsg, LastFailedAt: t1}
	d2 := &DeadTask{ID: m2.ID, Type: m2.Type, Payload: m2.Payload, ErrorMsg: m2.ErrorMsg, LastFailedAt: t2}

	type deadEntry struct {
		msg          *taskMessage
		lastFailedAt time.Time
	}
	tests := []struct {
		dead []deadEntry
		want []*DeadTask
	}{
		{
			dead: []deadEntry{
				{m1, t1},
				{m2, t2},
			},
			want: []*DeadTask{d1, d2},
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
		for _, d := range tc.dead {
			err := r.client.ZAdd(dead, &redis.Z{
				Member: mustMarshal(t, d.msg),
				Score:  float64(d.lastFailedAt.Unix())}).Err()
			if err != nil {
				t.Fatal(err)
			}
		}

		got, err := inspector.ListDeadTasks()
		if err != nil {
			t.Errorf("(*Inspector).ListDeadTask = %v, %v; want %v, nil", got, err, tc.want)
			continue
		}

		sortOpt := cmp.Transformer("SortDeadTasks", func(in []*DeadTask) []*DeadTask {
			out := append([]*DeadTask(nil), in...) // Copy input to avoid mutating it
			sort.Slice(out, func(i, j int) bool {
				return out[i].ID.String() < out[j].ID.String()
			})
			return out
		})
		timeCmpOpt := EquateApproxTime(time.Second)
		if diff := cmp.Diff(tc.want, got, sortOpt, timeCmpOpt); diff != "" {
			t.Errorf("(*Inspector).ListDeadTasks = %v, want %v; (-want, +got)\n%s",
				got, tc.want, diff)
			continue
		}
	}
}
