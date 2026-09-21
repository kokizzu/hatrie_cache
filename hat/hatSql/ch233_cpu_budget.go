package hatSql

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrSQLCPUTimeExceeded identifies cooperative cancellation after a query
	// consumed its configured CPU-time budget.
	ErrSQLCPUTimeExceeded = errors.New("SQL query CPU time limit exceeded")
	// ErrSQLCPUTimeUnsupported indicates that the selected platform has no
	// thread CPU-time clock for the opt-in budget.
	ErrSQLCPUTimeUnsupported = errors.New("SQL query CPU time clock unsupported")
)

type sqlCPUClock func() (threadID int64, cpuTime time.Duration, err error)

type sqlCPUTimeTracker struct {
	mu    sync.Mutex
	last  map[int64]time.Duration
	total time.Duration
}

func (tracker *sqlCPUTimeTracker) observe(clock sqlCPUClock) (time.Duration, error) {
	threadID, current, err := clock()
	if err != nil {
		return 0, err
	}
	if current < 0 {
		return 0, fmt.Errorf("SQL CPU time clock returned a negative duration")
	}

	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.last == nil {
		tracker.last = make(map[int64]time.Duration, 1)
	}
	if previous, ok := tracker.last[threadID]; ok && current > previous {
		delta := current - previous
		const maxDuration = time.Duration(1<<63 - 1)
		if maxDuration-tracker.total < delta {
			tracker.total = maxDuration
		} else {
			tracker.total += delta
		}
	}
	tracker.last[threadID] = current
	return tracker.total, nil
}

func (control *sqlExecutionControl) checkCPUTime() error {
	if !control.consumeCPUTimeCheck() {
		return nil
	}
	used, err := control.cpuTime.observe(control.cpuClock)
	if err != nil {
		return fmt.Errorf("SQL CPU time budget check failed: %w", err)
	}
	if used >= control.maxCPUTime {
		return fmt.Errorf("%w: %w (used %s, maximum %s)", ErrSQLCPUTimeExceeded, context.Canceled, used, control.maxCPUTime)
	}
	return nil
}

func (control *sqlExecutionControl) consumeCPUTimeCheck() bool {
	if control.cpuTimeEvery <= 1 {
		return true
	}
	for {
		remaining := control.cpuTimeFuel.Load()
		if remaining > 1 {
			if control.cpuTimeFuel.CompareAndSwap(remaining, remaining-1) {
				return false
			}
			continue
		}
		if control.cpuTimeFuel.CompareAndSwap(remaining, control.cpuTimeEvery) {
			return true
		}
	}
}
