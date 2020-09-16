package mocktime

import (
	"sync"
	"sync/atomic"
	"time"
)

/*
TODO(brett19): Solve the race condition that exists in the timers.
There exists a race condition when it comes to time-travelling here.  Technically
time-travelling is expected by consumers to be a synchronous thing.  IE: If you set
a timer, and then time travel past its execution time, that it will have executed.
Unfortunately timers are executed asynchronously, so it's possible that they will
not have been invoked synchronously, for instance if they were triggered on the
very precipice of the time travel call, they might still be pending...
This has been worked around currently by waiting for 5ms after a time travel to
allow any pending timers to wake up and execute.
*/

type mockTimer struct {
	timer       *time.Timer
	triggerTime time.Time
}

// Chrono represents special time handling, allowing the
// acceleration or alteration of time.
type Chrono struct {
	timeShiftNs uint64
	timersLock  sync.Mutex
	timers      []*mockTimer
}

// Now returns the current timestamp from the mock time.
func (t *Chrono) Now() time.Time {
	timeShiftNs := atomic.LoadUint64(&t.timeShiftNs)
	timeShift := time.Duration(timeShiftNs) * time.Nanosecond
	return time.Now().Add(timeShift)
}

// TimeTravel adjusts the current time by a set amount
func (t *Chrono) TimeTravel(d time.Duration) {
	if d < 0 {
		d = 0
	}

	timeShiftNs := uint64(d / time.Nanosecond)
	atomic.AddUint64(&t.timeShiftNs, timeShiftNs)

	t.checkTimers()
}

func (t *Chrono) addTimer(mtmr *mockTimer) {
	t.timersLock.Lock()
	defer t.timersLock.Unlock()

	t.timers = append(t.timers, mtmr)
}

func (t *Chrono) removeTimer(tmr *mockTimer) {
	t.timersLock.Lock()
	defer t.timersLock.Unlock()
}

func (t *Chrono) findExpiredTimers() []*mockTimer {
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

func (t *Chrono) checkTimers() {
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

	// We sleep for 5ms here to allow any pending timers to be executed.  Hopefully
	// thats enough time to allow them all to execute, but who knows...
	time.Sleep(5 * time.Millisecond)
}

// AfterFunc calls a callback function after a duration has passed.
func (t *Chrono) AfterFunc(d time.Duration, f func()) {
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
func (t *Chrono) After(d time.Duration) <-chan time.Time {
	tmrCh := make(chan time.Time)
	t.AfterFunc(d, func() {
		tmrCh <- t.Now()
	})
	return tmrCh
}
