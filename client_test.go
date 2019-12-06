package asynq

import (
	"github.com/hibiken/asynq/internal/rdb"
	"testing"
	"time"
)

func TestClient(t *testing.T) {
	r := setup(t)
	client := &Client{rdb.NewRDB(r)}

	tests := []struct {
		task              *Task
		processAt         time.Time
		wantQueueSize     int64
		wantScheduledSize int64
	}{
		{
			task:              &Task{Type: "send_email", Payload: map[string]interface{}{"to": "customer@gmail.com", "from": "merchant@example.com"}},
			processAt:         time.Now(),
			wantQueueSize:     1,
			wantScheduledSize: 0,
		},
		{
			task:              &Task{Type: "send_email", Payload: map[string]interface{}{"to": "customer@gmail.com", "from": "merchant@example.com"}},
			processAt:         time.Now().Add(2 * time.Hour),
			wantQueueSize:     0,
			wantScheduledSize: 1,
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}

		err := client.Process(tc.task, tc.processAt)
		if err != nil {
			t.Error(err)
			continue
		}

		if l := r.LLen(defaultQ).Val(); l != tc.wantQueueSize {
			t.Errorf("%q has length %d, want %d", defaultQ, l, tc.wantQueueSize)
		}

		if l := r.ZCard(scheduledQ).Val(); l != tc.wantScheduledSize {
			t.Errorf("%q has length %d, want %d", scheduledQ, l, tc.wantScheduledSize)
		}
	}
}
