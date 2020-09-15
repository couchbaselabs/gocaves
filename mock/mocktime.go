package mock

import (
	"sync"
	"sync/atomic"
	"time"
)

type mockTimer struct {
	timer       *time.Timer
	triggerTime time.Time
}

// MockTime represents special time handling, allowing the
// acceleration or alteration of time.
type MockTime struct {
	timeShiftNs uint64
	timersLock  sync.Mutex
	timers      []*mockTimer
}

// Now returns the current timestamp from the mock time.
func (t *MockTime) Now() time.Time {
	timeShiftNs := atomic.LoadUint64(&t.timeShiftNs)
	timeShift := time.Duration(timeShiftNs) * time.Nanosecond
	return time.Now().Add(timeShift)
}

// TimeTravel adjusts the current time by a set amount
func (t *MockTime) TimeTravel(d time.Duration) {
	if d < 0 {
		d = 0
	}

	timeShiftNs := uint64(d / time.Nanosecond)
	atomic.AddUint64(&t.timeShiftNs, timeShiftNs)

	t.checkTimers()
}

func (t *MockTime) addTimer(mtmr *mockTimer) {
	t.timersLock.Lock()
	defer t.timersLock.Unlock()

	t.timers = append(t.timers, mtmr)
}

func (t *MockTime) removeTimer(tmr *mockTimer) {
	t.timersLock.Lock()
	defer t.timersLock.Unlock()
}

func (t *MockTime) findExpiredTimers() []*mockTimer {
	curTime := t.Now()

	t.timersLock.Lock()
	defer t.timersLock.Unlock()

	var expiredTimers []*mockTimer
	for _, mtmr := range t.timers {
		if mtmr.triggerTime.After(curTime) {
			continue
		}

		expiredTimers = append(expiredTimers, mtmr)
	}

	return expiredTimers
}

func (t *MockTime) checkTimers() {
	// We intentionally split this method into two pieces, one which scans
	// for the expired timers, and the second stage which actually resets them
	// for immediately which causes them to trigger.  This is important as the
	// processing of a timer requires locking the timer list to remove it, which
	// could cause a race.  The timers themselves are thread-safe though.
	expiredTimers := t.findExpiredTimers()

	for _, mtmr := range expiredTimers {
		// If Stop returns false, it means it already has triggered and could not
		// be stopped, but this is fine and we can simply ignore it since all we
		// are trying to do here is reset it to trigger immediately anyways.
		if mtmr.timer.Stop() {
			mtmr.timer.Reset(0)
		}
	}
}

// AfterFunc calls a callback function after a duration has passed.
func (t *MockTime) AfterFunc(d time.Duration, f func()) {
	triggerTime := time.Now().Add(d)

	mtmr := &mockTimer{
		triggerTime: triggerTime,
	}
	mtmr.timer = time.AfterFunc(d, func() {
		t.removeTimer(mtmr)
		f()
	})
	t.addTimer(mtmr)
}

// After returns a channel which signals when the duration has passed.
func (t *MockTime) After(d time.Duration) <-chan time.Time {
	tmrCh := make(chan time.Time)
	t.AfterFunc(d, func() {
		tmrCh <- t.Now()
	})
	return tmrCh
}
