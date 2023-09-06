// Copyright 2022 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package dash

import (
	"testing"
	"time"

	"github.com/Kua-Fu/asynq"
	"github.com/gdamore/tcell/v2"
	"github.com/google/go-cmp/cmp"
)

func makeKeyEventHandler(t *testing.T, state *State) *keyEventHandler {
	ticker := time.NewTicker(time.Second)
	t.Cleanup(func() { ticker.Stop() })
	return &keyEventHandler{
		s:            tcell.NewSimulationScreen("UTF-8"),
		state:        state,
		done:         make(chan struct{}),
		fetcher:      &fakeFetcher{},
		drawer:       &fakeDrawer{},
		ticker:       ticker,
		pollInterval: time.Second,
	}
}

type keyEventHandlerTest struct {
	desc      string            // test description
	state     *State            // initial state, to be mutated by the handler
	events    []*tcell.EventKey // keyboard events
	wantState State             // expected state after the events
}

func TestKeyEventHandler(t *testing.T) {
	tests := []*keyEventHandlerTest{
		{
			desc:      "navigates to help view",
			state:     &State{view: viewTypeQueues},
			events:    []*tcell.EventKey{tcell.NewEventKey(tcell.KeyRune, '?', tcell.ModNone)},
			wantState: State{view: viewTypeHelp},
		},
		{
			desc: "navigates to queue details view",
			state: &State{
				view: viewTypeQueues,
				queues: []*asynq.QueueInfo{
					{Queue: "default", Size: 100, Active: 10, Pending: 40, Scheduled: 40, Completed: 10},
				},
				queueTableRowIdx: 0,
			},
			events: []*tcell.EventKey{
				tcell.NewEventKey(tcell.KeyRune, 'j', tcell.ModNone),   // down
				tcell.NewEventKey(tcell.KeyEnter, '\n', tcell.ModNone), // Enter
			},
			wantState: State{
				view: viewTypeQueueDetails,
				queues: []*asynq.QueueInfo{
					{Queue: "default", Size: 100, Active: 10, Pending: 40, Scheduled: 40, Completed: 10},
				},
				selectedQueue:    &asynq.QueueInfo{Queue: "default", Size: 100, Active: 10, Pending: 40, Scheduled: 40, Completed: 10},
				queueTableRowIdx: 1,
				taskState:        asynq.TaskStateActive,
				pageNum:          1,
			},
		},
		{
			desc: "does nothing if no queues are present",
			state: &State{
				view:             viewTypeQueues,
				queues:           []*asynq.QueueInfo{}, // empty
				queueTableRowIdx: 0,
			},
			events: []*tcell.EventKey{
				tcell.NewEventKey(tcell.KeyRune, 'j', tcell.ModNone),   // down
				tcell.NewEventKey(tcell.KeyEnter, '\n', tcell.ModNone), // Enter
			},
			wantState: State{
				view:             viewTypeQueues,
				queues:           []*asynq.QueueInfo{},
				queueTableRowIdx: 0,
			},
		},
		{
			desc: "opens task info modal",
			state: &State{
				view: viewTypeQueueDetails,
				queues: []*asynq.QueueInfo{
					{Queue: "default", Size: 500, Active: 10, Pending: 40},
				},
				queueTableRowIdx: 1,
				selectedQueue:    &asynq.QueueInfo{Queue: "default", Size: 50, Active: 10, Pending: 40},
				taskState:        asynq.TaskStatePending,
				pageNum:          1,
				tasks: []*asynq.TaskInfo{
					{ID: "xxxx", Type: "foo"},
					{ID: "yyyy", Type: "bar"},
					{ID: "zzzz", Type: "baz"},
				},
				taskTableRowIdx: 2,
			},
			events: []*tcell.EventKey{
				tcell.NewEventKey(tcell.KeyEnter, '\n', tcell.ModNone), // Enter
			},
			wantState: State{
				view: viewTypeQueueDetails,
				queues: []*asynq.QueueInfo{
					{Queue: "default", Size: 500, Active: 10, Pending: 40},
				},
				queueTableRowIdx: 1,
				selectedQueue:    &asynq.QueueInfo{Queue: "default", Size: 50, Active: 10, Pending: 40},
				taskState:        asynq.TaskStatePending,
				pageNum:          1,
				tasks: []*asynq.TaskInfo{
					{ID: "xxxx", Type: "foo"},
					{ID: "yyyy", Type: "bar"},
					{ID: "zzzz", Type: "baz"},
				},
				taskTableRowIdx: 2,
				// new states
				taskID:       "yyyy",
				selectedTask: &asynq.TaskInfo{ID: "yyyy", Type: "bar"},
			},
		},
		{
			desc: "Esc closes task info modal",
			state: &State{
				view: viewTypeQueueDetails,
				queues: []*asynq.QueueInfo{
					{Queue: "default", Size: 500, Active: 10, Pending: 40},
				},
				queueTableRowIdx: 1,
				selectedQueue:    &asynq.QueueInfo{Queue: "default", Size: 50, Active: 10, Pending: 40},
				taskState:        asynq.TaskStatePending,
				pageNum:          1,
				tasks: []*asynq.TaskInfo{
					{ID: "xxxx", Type: "foo"},
					{ID: "yyyy", Type: "bar"},
					{ID: "zzzz", Type: "baz"},
				},
				taskTableRowIdx: 2,
				taskID:          "yyyy", // presence of this field opens the modal
			},
			events: []*tcell.EventKey{
				tcell.NewEventKey(tcell.KeyEscape, ' ', tcell.ModNone), // Esc
			},
			wantState: State{
				view: viewTypeQueueDetails,
				queues: []*asynq.QueueInfo{
					{Queue: "default", Size: 500, Active: 10, Pending: 40},
				},
				queueTableRowIdx: 1,
				selectedQueue:    &asynq.QueueInfo{Queue: "default", Size: 50, Active: 10, Pending: 40},
				taskState:        asynq.TaskStatePending,
				pageNum:          1,
				tasks: []*asynq.TaskInfo{
					{ID: "xxxx", Type: "foo"},
					{ID: "yyyy", Type: "bar"},
					{ID: "zzzz", Type: "baz"},
				},
				taskTableRowIdx: 2,
				taskID:          "", // this field should be unset
			},
		},
		{
			desc: "Arrow keys are disabled while task info modal is open",
			state: &State{
				view: viewTypeQueueDetails,
				queues: []*asynq.QueueInfo{
					{Queue: "default", Size: 500, Active: 10, Pending: 40},
				},
				queueTableRowIdx: 1,
				selectedQueue:    &asynq.QueueInfo{Queue: "default", Size: 50, Active: 10, Pending: 40},
				taskState:        asynq.TaskStatePending,
				pageNum:          1,
				tasks: []*asynq.TaskInfo{
					{ID: "xxxx", Type: "foo"},
					{ID: "yyyy", Type: "bar"},
					{ID: "zzzz", Type: "baz"},
				},
				taskTableRowIdx: 2,
				taskID:          "yyyy", // presence of this field opens the modal
			},
			events: []*tcell.EventKey{
				tcell.NewEventKey(tcell.KeyLeft, ' ', tcell.ModNone),
			},

			// no change
			wantState: State{
				view: viewTypeQueueDetails,
				queues: []*asynq.QueueInfo{
					{Queue: "default", Size: 500, Active: 10, Pending: 40},
				},
				queueTableRowIdx: 1,
				selectedQueue:    &asynq.QueueInfo{Queue: "default", Size: 50, Active: 10, Pending: 40},
				taskState:        asynq.TaskStatePending,
				pageNum:          1,
				tasks: []*asynq.TaskInfo{
					{ID: "xxxx", Type: "foo"},
					{ID: "yyyy", Type: "bar"},
					{ID: "zzzz", Type: "baz"},
				},
				taskTableRowIdx: 2,
				taskID:          "yyyy", // presence of this field opens the modal
			},
		},
		// TODO: Add more tests
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			h := makeKeyEventHandler(t, tc.state)
			for _, e := range tc.events {
				h.HandleKeyEvent(e)
			}
			if diff := cmp.Diff(tc.wantState, *tc.state, cmp.AllowUnexported(State{})); diff != "" {
				t.Errorf("after state was %+v, want %+v: (-want,+got)\n%s", *tc.state, tc.wantState, diff)
			}
		})
	}

}

/*** fake implementation for tests ***/

type fakeFetcher struct{}

func (f *fakeFetcher) Fetch(s *State) {}

type fakeDrawer struct{}

func (d *fakeDrawer) Draw(s *State) {}
