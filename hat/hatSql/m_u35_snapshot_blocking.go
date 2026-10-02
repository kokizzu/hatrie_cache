package hatSql

import "context"

// WaitUntil blocks until every configured source partition has been observed
// and the common frontier reaches target. It uses update notifications rather
// than polling or a background worker. A target of zero still waits for the
// first observation from every partition so zero remains distinguishable from
// an uninitialized tracker.
func (tracker *SQLSourceFrontierTracker) WaitUntil(ctx context.Context, target uint64) error {
	if tracker == nil {
		return ErrSQLSourceFrontierTrackerNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if tracker.ReadyAt(target) {
		return nil
	}
	for {
		tracker.mu.Lock()
		if tracker.observedCount == len(tracker.states) && len(tracker.frontierHeap) > 0 && tracker.states[tracker.frontierHeap[0]].frontier >= target {
			tracker.mu.Unlock()
			return nil
		}
		if err := ctx.Err(); err != nil {
			tracker.mu.Unlock()
			return err
		}
		if tracker.notify == nil {
			tracker.notify = make(chan struct{})
		}
		notify := tracker.notify
		tracker.mu.Unlock()

		select {
		case <-notify:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
