package hatCache

import (
	"context"
	"fmt"
)

// WaitSQLJSONIndexReady synchronously rebuilds one configured SQL JSON index
// when it is missing or stale, then returns its current maintenance state.
// Rebuild work remains cooperative and observes ctx between queued rebuilds.
// A caller can use this as a startup or deployment readiness barrier without
// starting a background index worker.
func (ht *HatTrie) WaitSQLJSONIndexReady(ctx context.Context, key, field string) (SQLJSONIndexMaintenanceStats, bool, error) {
	if ht == nil {
		return SQLJSONIndexMaintenanceStats{}, false, ErrNilHatTrie
	}
	if ctx == nil {
		return SQLJSONIndexMaintenanceStats{}, false, fmt.Errorf("SQL JSON index readiness context is nil")
	}
	for {
		stats, available, err := ht.SQLJSONIndexMaintenanceStats(key, field)
		if err != nil || !available || stats.Current {
			return stats, available, err
		}
		if err := ctx.Err(); err != nil {
			return stats, true, err
		}
		if err := ht.ScheduleSQLJSONIndexRebuild(key, field); err != nil {
			return stats, true, err
		}
		if _, err := ht.RunScheduledSQLJSONIndexRebuildsWithProgress(ctx, 1, nil); err != nil {
			return stats, true, err
		}
	}
}
