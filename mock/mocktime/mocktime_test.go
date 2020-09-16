package mocktime

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestBasic(t *testing.T) {
	// Checks that the basics work
	chrono := &Chrono{}

	var afterFnCalls int32
	chrono.AfterFunc(50*time.Millisecond, func() {
		atomic.AddInt32(&afterFnCalls, 1)
	})

	if atomic.LoadInt32(&afterFnCalls) != 0 {
		t.Errorf("after func was invoked immediately in error")
	}

	chrono.TimeTravel(40 * time.Millisecond)

	if atomic.LoadInt32(&afterFnCalls) != 0 {
		t.Errorf("after func was invoked early in error")
	}

	chrono.TimeTravel(20 * time.Millisecond)

	if atomic.LoadInt32(&afterFnCalls) != 1 {
		t.Errorf("after func was not invoked after its timeout")
	}

	chrono.TimeTravel(4000 * time.Millisecond)

	if atomic.LoadInt32(&afterFnCalls) != 1 {
		t.Errorf("after func was invoked more than once")
	}
}

func TestScheduled(t *testing.T) {
	// Checks that timers are scheduled for real time correctly. Basically
	// it checks that timers work like normal without time travelling.
	chrono := &Chrono{}

	var afterFnCalls int32
	chrono.AfterFunc(50*time.Millisecond, func() {
		atomic.AddInt32(&afterFnCalls, 1)
	})

	time.Sleep(60 * time.Millisecond)

	if atomic.LoadInt32(&afterFnCalls) != 1 {
		t.Errorf("after func was not invoked after its timeout")
	}

	time.Sleep(150 * time.Millisecond)

	if atomic.LoadInt32(&afterFnCalls) != 1 {
		t.Errorf("after func was invoked more than once")
	}
}

func TestRescheduled(t *testing.T) {
	// Checks that timers are rescheduled correctly after a time travel
	// so that they are invoked when they should be.
	chrono := &Chrono{}

	var afterFnCalls int32
	chrono.AfterFunc(50*time.Millisecond, func() {
		atomic.AddInt32(&afterFnCalls, 1)
	})

	chrono.TimeTravel(40 * time.Millisecond)

	if atomic.LoadInt32(&afterFnCalls) != 0 {
		t.Errorf("after func was invoked early in error")
	}

	time.Sleep(20 * time.Millisecond)

	if atomic.LoadInt32(&afterFnCalls) != 1 {
		t.Errorf("after func was not invoked after its timeout")
	}

	chrono.TimeTravel(4000 * time.Millisecond)

	if atomic.LoadInt32(&afterFnCalls) != 1 {
		t.Errorf("after func was invoked more than once")
	}
}
