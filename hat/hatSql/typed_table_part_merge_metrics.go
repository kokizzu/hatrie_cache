package hatSql

import "time"

// TypedTablePartMergeMetrics is a zero-allocation snapshot of deferred
// patch-part pressure and compaction work. RowsRead and RowsWritten are the
// cumulative physical rows scanned and retained by completed merges.
// WriteAmplification is cumulative rows written per deleted row; it is zero
// until a merge has removed at least one row.
type TypedTablePartMergeMetrics struct {
	PendingDeletes     int           `json:"pending_deletes"`
	OldestPendingAge   time.Duration `json:"oldest_pending_age"`
	MergeCount         uint64        `json:"merge_count"`
	RowsRead           uint64        `json:"rows_read"`
	RowsWritten        uint64        `json:"rows_written"`
	DeletedRows        uint64        `json:"deleted_rows"`
	WriteAmplification float64       `json:"write_amplification"`
	LastMergeAt        time.Time     `json:"last_merge_at"`
	LastMergeDuration  time.Duration `json:"last_merge_duration"`
}

// PartMergeMetrics returns the current patch-part backlog and cumulative
// physical merge accounting. The snapshot is protected by the table read lock
// and does not clone rows, events, or bitmaps. Patch parts are opt-in, so the
// zero value is returned for a table using the default physical-delete path.
func (table *TypedTable) PartMergeMetrics() TypedTablePartMergeMetrics {
	if table == nil {
		return TypedTablePartMergeMetrics{}
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	state := table.patchParts
	if state == nil {
		return TypedTablePartMergeMetrics{}
	}
	metrics := TypedTablePartMergeMetrics{
		PendingDeletes:    state.deletedCount,
		MergeCount:        state.mergeCount,
		RowsRead:          state.mergeInputRows,
		RowsWritten:       state.mergeOutputRows,
		DeletedRows:       state.mergeDeletedRows,
		LastMergeAt:       state.lastMergeAt,
		LastMergeDuration: state.lastMergeDuration,
	}
	if !state.pendingSince.IsZero() {
		age := state.nowUTC().Sub(state.pendingSince)
		if age > 0 {
			metrics.OldestPendingAge = age
		}
	}
	if metrics.DeletedRows > 0 {
		metrics.WriteAmplification = float64(metrics.RowsWritten) / float64(metrics.DeletedRows)
	}
	return metrics
}
