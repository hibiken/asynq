// Copyright 2022 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package dash

import (
	"testing"

	"github.com/gdamore/tcell/v2"
	"github.com/google/go-cmp/cmp"
	"github.com/hibiken/asynq"
)

func makeKeyEventHandler(state *State) *keyEventHandler {
	return &keyEventHandler{
		s:       tcell.NewSimulationScreen("UTF-8"),
		state:   state,
		done:    make(chan struct{}),
		fetcher: &fakeFetcher{},
		drawer:  &fakeDrawer{},
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
			desc:      "navigates to help page",
			state:     &State{view: viewTypeQueues},
			events:    []*tcell.EventKey{tcell.NewEventKey(tcell.KeyRune, '?', tcell.ModNone)},
			wantState: State{view: viewTypeHelp},
		},
		// TODO: Add more tests
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			h := makeKeyEventHandler(tc.state)
			for _, e := range tc.events {
				h.HandleKeyEvent(e)
			}
			if diff := cmp.Diff(tc.wantState, *tc.state, cmp.AllowUnexported(State{}, redisInfo{})); diff != "" {
				t.Errorf("after state was %+v, want %+v: (-want,+got)\n%s", *tc.state, tc.wantState, diff)
			}
		})
	}

}

/*** fake implementation for tests ***/

type fakeFetcher struct{}

func (f *fakeFetcher) fetchQueues()                                                              {}
func (f *fakeFetcher) fetchQueueInfo(qname string)                                               {}
func (f *fakeFetcher) fetchRedisInfo()                                                           {}
func (f *fakeFetcher) fetchTasks(qname string, taskState asynq.TaskState, pageSize, pageNum int) {}
func (f *fakeFetcher) fetchAggregatingTasks(qname, group string, pageSize, pageNum int)          {}
func (f *fakeFetcher) fetchGroups(qname string)                                                  {}

type fakeDrawer struct{}

func (d *fakeDrawer) draw(s *State) {}
