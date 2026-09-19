package hatSql

import (
	"errors"
	"time"
)

const typedTablePatchPartsDefaultMergeThreshold = 1024

// TypedTablePatchOptions configures the optional logical-delete path. When
// enabled, deletes publish a tombstone and defer physical row movement until
// the merge threshold is reached.
type TypedTablePatchOptions struct {
	Enabled        bool
	MergeThreshold int
}

var ErrTypedTablePatchPartsDisabled = errors.New("typed table patch parts disabled")

type typedTablePatchState struct {
	deleted        typedTableDeleteBitmap
	deletedCount   int
	mergeThreshold int
	mergeScheduled bool
}

func normalizeTypedTablePatchOptions(options TypedTablePatchOptions) TypedTablePatchOptions {
	if !options.Enabled {
		return TypedTablePatchOptions{}
	}
	if options.MergeThreshold <= 0 {
		options.MergeThreshold = typedTablePatchPartsDefaultMergeThreshold
	}
	return options
}

func newTypedTablePatchState(options TypedTablePatchOptions) *typedTablePatchState {
	if !options.Enabled {
		return nil
	}
	return &typedTablePatchState{mergeThreshold: options.MergeThreshold}
}

func (table *TypedTable) typedTableRowDeletedLocked(index int) bool {
	return table.patchParts != nil && table.patchParts.deleted.contains(index)
}

func (table *TypedTable) scheduleTypedTablePatchCompactionLocked() {
	state := table.patchParts
	if state == nil || state.mergeScheduled || state.deletedCount < state.mergeThreshold {
		return
	}
	state.mergeScheduled = true
	go table.runTypedTablePatchCompaction()
}

func (table *TypedTable) runTypedTablePatchCompaction() {
	table.mu.Lock()
	defer table.mu.Unlock()
	state := table.patchParts
	if state == nil {
		return
	}
	if state.deletedCount >= state.mergeThreshold {
		table.compactTypedTablePatchPartsLocked()
	}
	state.mergeScheduled = false
}

// CompactPatchParts immediately folds logical deletes into the compact row
// arrays. It is safe to call while readers and writers use the table lock.
func (table *TypedTable) CompactPatchParts() error {
	if table == nil {
		return errors.New("typed table is nil")
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	if table.patchParts == nil {
		return ErrTypedTablePatchPartsDisabled
	}
	table.compactTypedTablePatchPartsLocked()
	table.patchParts.mergeScheduled = false
	return nil
}

func (table *TypedTable) compactTypedTablePatchPartsLocked() {
	state := table.patchParts
	if state == nil || state.deletedCount == 0 {
		return
	}
	physicalRowsBefore := len(table.keys)
	deletedRows := state.deletedCount
	started := time.Time{}
	if table.storageEvents != nil {
		started = time.Now()
	}
	table.clearColumnarLayoutsLocked()
	write := 0
	for read, key := range table.keys {
		if state.deleted.contains(read) {
			if table.memoryBudgetMaxBytes > 0 {
				table.memoryBytes -= table.memoryRowBytes[read]
			}
			delete(table.positions, key)
			continue
		}
		if write != read {
			table.keys[write] = key
			table.positions[key] = write
			for column := range table.columns {
				table.columns[column].copy(write, read)
			}
			if table.memoryBudgetMaxBytes > 0 {
				table.memoryRowBytes[write] = table.memoryRowBytes[read]
			}
			if table.ttl != nil && table.ttl.options.Mode == TypedTableTTLProcessingTime {
				table.ttl.deadlines[write] = table.ttl.deadlines[read]
			}
			table.moveTypedTableColumnTTLDeadlineLocked(write, read)
		}
		state.deleted.clear(write)
		write++
	}
	table.keys = table.keys[:write]
	state.deleted.truncate(write)
	state.deletedCount = 0
	for column := range table.columns {
		table.columns[column].truncate(write)
	}
	if table.memoryBudgetMaxBytes > 0 {
		table.memoryRowBytes = table.memoryRowBytes[:write]
	}
	table.truncateTypedTableColumnTTLDeadlinesLocked(write)
	if table.ttl != nil {
		if table.ttl.options.Mode == TypedTableTTLProcessingTime {
			table.ttl.deadlines = table.ttl.deadlines[:write]
		}
		table.rebuildTypedTableTTLExpiryIndexLocked(write)
	}
	if table.storageEvents != nil {
		table.recordStorageEventLocked(TypedTableStorageEventPatchPartMerged, physicalRowsBefore, write, 0, deletedRows, time.Since(started))
	}
}
