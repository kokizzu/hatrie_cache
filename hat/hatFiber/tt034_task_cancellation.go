package hatFiber

import "context"

// DrainState describes the scheduler shutdown lifecycle.
type DrainState uint8

const (
	// DrainStateOpen accepts new fibers and has not requested cancellation.
	DrainStateOpen DrainState = iota + 1
	// DrainStateRequested has canceled active fibers but may still contain
	// canceled entries in the ready queue.
	DrainStateRequested
	// DrainStateComplete has no active fibers or queued cancellation entries.
	DrainStateComplete
)

func (state DrainState) String() string {
	switch state {
	case DrainStateOpen:
		return "open"
	case DrainStateRequested:
		return "requested"
	case DrainStateComplete:
		return "complete"
	default:
		return "unknown"
	}
}

// Context returns the scheduler-owned cancellation token. It is shared by
// every callback when callers pass it to Run and is canceled by Close or
// Drain. The token is stable for the scheduler lifetime.
func (scheduler *Scheduler) Context() context.Context {
	if scheduler == nil || scheduler.lifecycle == nil {
		return context.Background()
	}
	return scheduler.lifecycle
}

// DrainState returns the current shutdown lifecycle state.
func (scheduler *Scheduler) DrainState() DrainState {
	if scheduler == nil || scheduler.lifecycle == nil {
		return DrainStateComplete
	}
	return scheduler.drainState
}

// Drain requests cancellation and consumes canceled ready-queue entries. It
// does not invoke canceled callbacks or reap terminal fibers, so callers can
// inspect their status and release their IDs explicitly. A canceled caller
// context leaves the scheduler in DrainStateRequested and can be retried.
func (scheduler *Scheduler) Drain(ctx context.Context) (RunStats, error) {
	var stats RunStats
	if scheduler == nil {
		return stats, ErrSchedulerClosed
	}
	if ctx == nil {
		return stats, ErrNilContext
	}
	scheduler.Close()
	stats, err := scheduler.Run(ctx, 0)
	if err != nil {
		return stats, err
	}
	if scheduler.readyCount == 0 && scheduler.activeFiberCount() == 0 {
		scheduler.drainState = DrainStateComplete
	}
	return stats, nil
}

func (scheduler *Scheduler) requestCancellation() {
	if scheduler.cancel != nil {
		scheduler.cancel()
	}
	if scheduler.lifecycle != nil && scheduler.drainState == DrainStateOpen {
		scheduler.drainState = DrainStateRequested
	}
	for index := range scheduler.slots {
		slot := &scheduler.slots[index]
		if slot.status != StatusReady && slot.status != StatusWaiting && slot.status != StatusRunning {
			continue
		}
		slot.status = StatusCancelled
		slot.err = context.Canceled
		slot.function = nil
		scheduler.clearFiberLocals(uint32(index))
	}
}

func (scheduler *Scheduler) activeFiberCount() int {
	active := 0
	for index := range scheduler.slots {
		switch scheduler.slots[index].status {
		case StatusReady, StatusRunning, StatusWaiting:
			active++
		}
	}
	return active
}
