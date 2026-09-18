package hatCache

import "context"

// ReplicationApplyThrottle gates an already ordered replication batch before
// it mutates the local trie. A nil throttle preserves the legacy path.
type ReplicationApplyThrottle interface {
	Wait(context.Context, int) error
}

func waitForReplicationApply(ctx context.Context, throttle ReplicationApplyThrottle, entries int) error {
	if throttle == nil || entries <= 0 {
		return nil
	}
	return throttle.Wait(ctx, entries)
}
