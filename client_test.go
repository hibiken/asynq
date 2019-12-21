package asynq

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/hibiken/asynq/internal/rdb"
)

func TestClient(t *testing.T) {
	r := setup(t)
	client := &Client{rdb.NewRDB(r)}

	task := &Task{Type: "send_email", Payload: map[string]interface{}{"to": "customer@gmail.com", "from": "merchant@example.com"}}

	tests := []struct {
		desc          string
		task          *Task
		processAt     time.Time
		opts          []Option
		wantEnqueued  []*rdb.TaskMessage
		wantScheduled []sortedSetEntry
	}{
		{
			desc:      "Process task immediately",
			task:      task,
			processAt: time.Now(),
			opts:      []Option{},
			wantEnqueued: []*rdb.TaskMessage{
				&rdb.TaskMessage{
					Type:    task.Type,
					Payload: task.Payload,
					Retry:   defaultMaxRetry,
					Queue:   "default",
				},
			},
			wantScheduled: nil, // db is flushed in setup so zset does not exist hence nil
		},
		{
			desc:         "Schedule task to be processed in the future",
			task:         task,
			processAt:    time.Now().Add(2 * time.Hour),
			opts:         []Option{},
			wantEnqueued: nil, // db is flushed in setup so list does not exist hence nil
			wantScheduled: []sortedSetEntry{
				{
					msg: &rdb.TaskMessage{
						Type:    task.Type,
						Payload: task.Payload,
						Retry:   defaultMaxRetry,
						Queue:   "default",
					},
					score: time.Now().Add(2 * time.Hour).Unix(),
				},
			},
		},
		{
			desc:      "Process task immediately with a custom retry count",
			task:      task,
			processAt: time.Now(),
			opts: []Option{
				MaxRetry(3),
			},
			wantEnqueued: []*rdb.TaskMessage{
				&rdb.TaskMessage{
					Type:    task.Type,
					Payload: task.Payload,
					Retry:   3,
					Queue:   "default",
				},
			},
			wantScheduled: nil, // db is flushed in setup so zset does not exist hence nil
		},
		{
			desc:      "Negative retry count",
			task:      task,
			processAt: time.Now(),
			opts: []Option{
				MaxRetry(-2),
			},
			wantEnqueued: []*rdb.TaskMessage{
				&rdb.TaskMessage{
					Type:    task.Type,
					Payload: task.Payload,
					Retry:   0, // Retry count should be set to zero
					Queue:   "default",
				},
			},
			wantScheduled: nil, // db is flushed in setup so zset does not exist hence nil
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}

		err := client.Process(tc.task, tc.processAt, tc.opts...)
		if err != nil {
			t.Error(err)
			continue
		}

		gotEnqueuedRaw := r.LRange(defaultQ, 0, -1).Val()
		gotEnqueued := mustUnmarshalSlice(t, gotEnqueuedRaw)
		if diff := cmp.Diff(tc.wantEnqueued, gotEnqueued, ignoreIDOpt); diff != "" {
			t.Errorf("%s;\nmismatch found in %q; (-want,+got)\n%s", tc.desc, defaultQ, diff)
		}

		gotScheduledRaw := r.ZRangeWithScores(scheduledQ, 0, -1).Val()
		var gotScheduled []sortedSetEntry
		for _, z := range gotScheduledRaw {
			gotScheduled = append(gotScheduled, sortedSetEntry{
				msg:   mustUnmarshal(t, z.Member.(string)),
				score: int64(z.Score),
			})
		}

		cmpOpt := cmp.AllowUnexported(sortedSetEntry{})
		if diff := cmp.Diff(tc.wantScheduled, gotScheduled, cmpOpt, ignoreIDOpt); diff != "" {
			t.Errorf("%s;\nmismatch found in %q; (-want,+got)\n%s", tc.desc, scheduledQ, diff)
		}
	}
}
