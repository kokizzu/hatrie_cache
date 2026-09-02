package hatSql

import (
	"errors"
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
	deleted        []bool
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
	return table.patchParts != nil && table.patchParts.deleted[index]
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
	table.clearColumnarLayoutsLocked()
	write := 0
	for read, key := range table.keys {
		if state.deleted[read] {
			delete(table.positions, key)
			continue
		}
		if write != read {
			table.keys[write] = key
			table.positions[key] = write
			for column := range table.columns {
				table.columns[column].copy(write, read)
			}
		}
		state.deleted[write] = false
		write++
	}
	table.keys = table.keys[:write]
	state.deleted = state.deleted[:write]
	state.deletedCount = 0
	for column := range table.columns {
		table.columns[column].truncate(write)
	}
}
