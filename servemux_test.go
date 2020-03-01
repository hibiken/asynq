// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"testing"
)

var called string

// makeFakeHandler returns a handler that updates the global called variable
// to the given identity.
func makeFakeHandler(identity string) Handler {
	return HandlerFunc(func(ctx context.Context, t *Task) error {
		called = identity
		return nil
	})
}

// A list of pattern, handler pair that is registered with mux.
var serveMuxRegister = []struct {
	pattern string
	h       Handler
}{
	{"email:", makeFakeHandler("default email handler")},
	{"email:signup", makeFakeHandler("signup email handler")},
	{"csv:export", makeFakeHandler("csv export handler")},
}

var serveMuxTests = []struct {
	typename string // task's type name
	want     string // identifier of the handler that should be called
}{
	{"email:signup", "signup email handler"},
	{"csv:export", "csv export handler"},
	{"email:daily", "default email handler"},
}

func TestServeMux(t *testing.T) {
	mux := NewServeMux()
	for _, e := range serveMuxRegister {
		mux.Handle(e.pattern, e.h)
	}

	for _, tc := range serveMuxTests {
		called = "" // reset to zero value

		task := NewTask(tc.typename, nil)
		if err := mux.ProcessTask(context.Background(), task); err != nil {
			t.Fatal(err)
		}

		if called != tc.want {
			t.Errorf("%q handler was called for task %q, want %q to be called", called, task.Type, tc.want)
		}
	}
}

func TestServeMuxRegisterNilHandler(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			t.Error("expected call to mux.HandleFunc to panic")
		}
	}()

	mux := NewServeMux()
	mux.HandleFunc("email:signup", nil)
}

func TestServeMuxRegisterEmptyPattern(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			t.Error("expected call to mux.HandleFunc to panic")
		}
	}()

	mux := NewServeMux()
	mux.Handle("", makeFakeHandler("email"))
}

func TestServeMuxRegisterDuplicatePattern(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			t.Error("expected call to mux.HandleFunc to panic")
		}
	}()

	mux := NewServeMux()
	mux.Handle("email", makeFakeHandler("email"))
	mux.Handle("email", makeFakeHandler("email:default"))
}

var notFoundTests = []struct {
	typename string // task's type name
}{
	{"image:minimize"},
	{"csv:"}, // registered patterns match the task's type prefix, not the other way around.
}

func TestServeMuxNotFound(t *testing.T) {
	mux := NewServeMux()
	for _, e := range serveMuxRegister {
		mux.Handle(e.pattern, e.h)
	}

	for _, tc := range notFoundTests {
		task := NewTask(tc.typename, nil)
		err := mux.ProcessTask(context.Background(), task)
		if err == nil {
			t.Errorf("ProcessTask did not return error for task %q, should return 'not found' error", task.Type)
		}
	}
}
