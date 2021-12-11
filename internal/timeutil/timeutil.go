// Package timeutil exports functions and types related to time and date.
package timeutil

import "time"

// A Clock is an object that can tell you the current time.
//
// This interface allows decoupling code that uses time from the code that creates
// a point in time. You can use this to your advantage by injecting Clocks into interfaces
// rather than having implementations call time.Now() directly.
//
// Use RealClock() in production.
// Use SimulatedClock() in test.
type Clock interface {
	Now() time.Time
}

func NewRealClock() Clock { return &realTimeClock{} }

type realTimeClock struct{}

func (_ *realTimeClock) Now() time.Time { return time.Now() }

// A SimulatedClock is a concrete Clock implementation that doesn't "tick" on its own.
// Time is advanced by explicit call to the AdvanceTime() or SetTime() functions.
type SimulatedClock struct {
	t time.Time
}

func NewSimulatedClock(t time.Time) *SimulatedClock {
	return &SimulatedClock{t}
}

func (c *SimulatedClock) Now() time.Time { return c.t }

func (c *SimulatedClock) SetTime(t time.Time) { c.t = t }

func (c *SimulatedClock) AdvanceTime(d time.Duration) { c.t.Add(d) }
