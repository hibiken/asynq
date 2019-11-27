package asynq

import (
	"fmt"
	"testing"
)

func TestPerform(t *testing.T) {
	tests := []struct {
		desc    string
		handler TaskHandler
		task    *Task
		wantErr bool
	}{
		{
			desc: "handler returns nil",
			handler: func(t *Task) error {
				fmt.Println("processing...")
				return nil
			},
			task:    &Task{Type: "gen_thumbnail", Payload: map[string]interface{}{"src": "some/img/path"}},
			wantErr: false,
		},
		{
			desc: "handler returns error",
			handler: func(t *Task) error {
				fmt.Println("processing...")
				return fmt.Errorf("something went wrong")
			},
			task:    &Task{Type: "gen_thumbnail", Payload: map[string]interface{}{"src": "some/img/path"}},
			wantErr: true,
		},
		{
			desc: "handler panics",
			handler: func(t *Task) error {
				fmt.Println("processing...")
				panic("something went terribly wrong")
			},
			task:    &Task{Type: "gen_thumbnail", Payload: map[string]interface{}{"src": "some/img/path"}},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		got := perform(tc.handler, tc.task)
		if !tc.wantErr && got != nil {
			t.Errorf("%s: perform() = %v, want nil", tc.desc, got)
			continue
		}
		if tc.wantErr && got == nil {
			t.Errorf("%s: perform() = nil, want non-nil error", tc.desc)
			continue
		}
	}
}
