package hatSql

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"sync"
)

const DefaultDifferentialRowNumberLagMaxRows = 1_000_000

var (
	ErrDifferentialRowNumberLagNil            = errors.New("hatSql: differential row-number/lag window is nil")
	ErrDifferentialRowNumberLagInvalid        = errors.New("hatSql: differential row-number/lag value is invalid")
	ErrDifferentialRowNumberLagMaxRowsInvalid = errors.New("hatSql: differential row-number/lag row limit is invalid")
	ErrDifferentialRowNumberLagMaxRows        = errors.New("hatSql: differential row-number/lag row limit exceeded")
	ErrDifferentialRowNumberLagMissing        = errors.New("hatSql: differential row-number/lag retraction is missing")
	ErrDifferentialRowNumberLagRowRequired    = errors.New("hatSql: differential row-number/lag insert requires a row")
	ErrDifferentialRowNumberLagRowMismatch    = errors.New("hatSql: differential row-number/lag row identity changed")
	ErrDifferentialRowNumberLagWeightOverflow = errors.New("hatSql: differential row-number/lag weight overflowed")
	ErrDifferentialRowNumberLagRowNumberLimit = errors.New("hatSql: differential row-number/lag row number overflowed")
)

// DifferentialRowNumberLagWindowOptions configures an exact differential
// ROW_NUMBER/LAG operator. Rows are ordered by Time, then Key, then the
// repeated-copy ordinal. A nil PartitionKey places all rows in one partition.
type DifferentialRowNumberLagWindowOptions struct {
	PartitionKey func(SQLRow) string
	Lag          int
	MaxRows      int
}

// DifferentialRowNumberLagRow is one current or corrective window result.
// Apply returns negative rows retracting obsolete results and positive rows
// adding current results. Ordinal identifies one copy of a weighted record.
type DifferentialRowNumberLagRow struct {
	Key       string
	Time      uint64
	Ordinal   uint64
	Diff      int64
	Partition string
	RowNumber uint64
	Row       SQLRow
	HasLag    bool
	LagRow    SQLRow
}

type differentialRowNumberLagID struct {
	key  string
	time uint64
}

type differentialRowNumberLagEntry struct {
	id        differentialRowNumberLagID
	partition string
	row       SQLRow
	weight    int64
}

type differentialRowNumberLagOutputID struct {
	partition string
	differentialRowNumberLagID
	ordinal uint64
}

// DifferentialRowNumberLagWindow maintains exact ROW_NUMBER and LAG results
// from signed differential updates. It is intentionally separate from the
// append-only IncrementalRowNumberLagWindow, whose low-memory monotone path is
// unchanged.
type DifferentialRowNumberLagWindow struct {
	mu           sync.RWMutex
	partitionKey func(SQLRow) string
	lag          int
	maxRows      int64
	totalRows    int64
	entries      map[differentialRowNumberLagID]differentialRowNumberLagEntry
	partitions   map[string][]differentialRowNumberLagID
}

// NewDifferentialRowNumberLagWindow validates options and creates an empty
// bounded differential row-number/lag operator.
func NewDifferentialRowNumberLagWindow(options DifferentialRowNumberLagWindowOptions) (*DifferentialRowNumberLagWindow, error) {
	if options.Lag < 0 {
		return nil, ErrDifferentialRowNumberLagInvalid
	}
	if options.MaxRows < 0 {
		return nil, ErrDifferentialRowNumberLagMaxRowsInvalid
	}
	if options.MaxRows == 0 {
		options.MaxRows = DefaultDifferentialRowNumberLagMaxRows
	}
	return &DifferentialRowNumberLagWindow{
		partitionKey: options.PartitionKey,
		lag:          options.Lag,
		maxRows:      int64(options.MaxRows),
		entries:      make(map[differentialRowNumberLagID]differentialRowNumberLagEntry),
		partitions:   make(map[string][]differentialRowNumberLagID),
	}, nil
}

// Apply applies signed weighted updates atomically and returns only the
// corrections caused by the batch. Retractions may arrive out of order and
// may remove part of a weighted record.
func (window *DifferentialRowNumberLagWindow) Apply(updates []DifferentialRow) ([]DifferentialRowNumberLagRow, error) {
	if window == nil {
		return nil, ErrDifferentialRowNumberLagNil
	}
	if len(updates) == 0 {
		return nil, nil
	}
	window.mu.Lock()
	defer window.mu.Unlock()
	if len(updates) == 1 && window.lag <= 1 {
		if output, handled, err := window.applyTailFastPath(updates[0]); handled || err != nil {
			return output, err
		}
	}
	if len(updates) == 1 {
		if output, handled, err := window.applySingleUpdate(updates[0]); handled || err != nil {
			return output, err
		}
	}

	workingEntries := make(map[differentialRowNumberLagID]differentialRowNumberLagEntry, len(window.entries)+len(updates))
	for id, entry := range window.entries {
		workingEntries[id] = entry
	}
	workingPartitions := make(map[string][]differentialRowNumberLagID, len(window.partitions))
	for partition, members := range window.partitions {
		workingPartitions[partition] = members
	}
	mutablePartitions := make(map[string]bool)
	clonePartition := func(partition string) []differentialRowNumberLagID {
		members := workingPartitions[partition]
		if !mutablePartitions[partition] {
			clone := make([]differentialRowNumberLagID, len(members), len(members)+1)
			copy(clone, members)
			members = clone
			workingPartitions[partition] = members
			mutablePartitions[partition] = true
		}
		if members == nil {
			members = make([]differentialRowNumberLagID, 0, 1)
			workingPartitions[partition] = members
		}
		return members
	}

	before := make(map[string][]DifferentialRowNumberLagRow)
	affected := make(map[string]struct{})
	ensureBefore := func(partition string) error {
		if _, exists := before[partition]; exists {
			return nil
		}
		rows, err := window.partitionRows(window.entries, window.partitions[partition], partition)
		if err != nil {
			return err
		}
		before[partition] = rows
		return nil
	}
	totalRows := window.totalRows

	for _, update := range updates {
		if update.Diff == 0 {
			continue
		}
		id := differentialRowNumberLagID{key: update.Key, time: update.Time}
		entry, exists := workingEntries[id]
		partition := ""
		if exists {
			partition = entry.partition
			if update.Row != nil && !reflect.DeepEqual(update.Row, entry.row) {
				return nil, fmt.Errorf("row %q at time %d: %w", update.Key, update.Time, ErrDifferentialRowNumberLagRowMismatch)
			}
			if update.Diff > 0 {
				if entry.weight > math.MaxInt64-update.Diff || totalRows > window.maxRows-update.Diff {
					return nil, ErrDifferentialRowNumberLagWeightOverflow
				}
			} else {
				if update.Diff == math.MinInt64 || entry.weight < -update.Diff {
					return nil, fmt.Errorf("row %q at time %d: %w", update.Key, update.Time, ErrDifferentialRowNumberLagMissing)
				}
			}
			if err := ensureBefore(partition); err != nil {
				return nil, err
			}
			affected[partition] = struct{}{}
			entry.weight += update.Diff
			totalRows += update.Diff
			if entry.weight == 0 {
				delete(workingEntries, id)
				members := differentialRowNumberLagRemoveID(clonePartition(partition), id)
				if len(members) == 0 {
					delete(workingPartitions, partition)
				} else {
					workingPartitions[partition] = members
				}
				continue
			}
			workingEntries[id] = entry
			continue
		}

		if update.Diff < 0 {
			return nil, fmt.Errorf("row %q at time %d: %w", update.Key, update.Time, ErrDifferentialRowNumberLagMissing)
		}
		if update.Row == nil {
			return nil, fmt.Errorf("row %q at time %d: %w", update.Key, update.Time, ErrDifferentialRowNumberLagRowRequired)
		}
		if update.Diff > window.maxRows || totalRows > window.maxRows-update.Diff {
			return nil, ErrDifferentialRowNumberLagMaxRows
		}
		partition = window.partition(update.Row)
		if err := ensureBefore(partition); err != nil {
			return nil, err
		}
		affected[partition] = struct{}{}
		workingEntries[id] = differentialRowNumberLagEntry{
			id:        id,
			partition: partition,
			row:       cloneDifferentialRowNumberLagSQLRow(update.Row),
			weight:    update.Diff,
		}
		members := clonePartition(partition)
		workingPartitions[partition] = differentialRowNumberLagInsertID(members, id)
		totalRows += update.Diff
	}

	if len(affected) == 0 {
		return nil, nil
	}
	after := make(map[string][]DifferentialRowNumberLagRow, len(affected))
	for partition := range affected {
		rows, err := window.partitionRows(workingEntries, workingPartitions[partition], partition)
		if err != nil {
			return nil, err
		}
		after[partition] = rows
	}
	output := differentialRowNumberLagCorrections(before, after)
	window.entries = workingEntries
	window.partitions = workingPartitions
	window.totalRows = totalRows
	return output, nil
}

// Snapshot returns the current positive window state in deterministic order.
func (window *DifferentialRowNumberLagWindow) Snapshot() []DifferentialRowNumberLagRow {
	rows, err := window.SnapshotWithError()
	if err != nil {
		return nil
	}
	return rows
}

// SnapshotWithError returns the current positive state and reports impossible
// internal row-number overflow instead of silently returning an empty result.
func (window *DifferentialRowNumberLagWindow) SnapshotWithError() ([]DifferentialRowNumberLagRow, error) {
	if window == nil {
		return nil, ErrDifferentialRowNumberLagNil
	}
	window.mu.RLock()
	defer window.mu.RUnlock()
	partitions := make([]string, 0, len(window.partitions))
	for partition := range window.partitions {
		partitions = append(partitions, partition)
	}
	sort.Strings(partitions)
	rows := make([]DifferentialRowNumberLagRow, 0, window.totalRows)
	for _, partition := range partitions {
		part, err := window.partitionRows(window.entries, window.partitions[partition], partition)
		if err != nil {
			return nil, err
		}
		rows = append(rows, part...)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return rows, nil
}

// Len returns the number of logical rows retained, including weighted copies.
func (window *DifferentialRowNumberLagWindow) Len() int {
	if window == nil {
		return 0
	}
	window.mu.RLock()
	defer window.mu.RUnlock()
	return int(window.totalRows)
}

func (window *DifferentialRowNumberLagWindow) partition(row SQLRow) string {
	if window.partitionKey == nil {
		return ""
	}
	return window.partitionKey(row)
}

func (window *DifferentialRowNumberLagWindow) applySingleUpdate(update DifferentialRow) ([]DifferentialRowNumberLagRow, bool, error) {
	if update.Diff == 0 {
		return nil, true, nil
	}
	id := differentialRowNumberLagID{key: update.Key, time: update.Time}
	entry, exists := window.entries[id]
	partition := ""
	if exists {
		partition = entry.partition
		if update.Row != nil && !reflect.DeepEqual(update.Row, entry.row) {
			return nil, true, fmt.Errorf("row %q at time %d: %w", update.Key, update.Time, ErrDifferentialRowNumberLagRowMismatch)
		}
		if update.Diff > 0 {
			if entry.weight > math.MaxInt64-update.Diff || window.totalRows > window.maxRows-update.Diff {
				return nil, true, ErrDifferentialRowNumberLagWeightOverflow
			}
		} else if update.Diff == math.MinInt64 || entry.weight < -update.Diff {
			return nil, true, fmt.Errorf("row %q at time %d: %w", update.Key, update.Time, ErrDifferentialRowNumberLagMissing)
		}
	} else {
		if update.Diff < 0 {
			return nil, true, fmt.Errorf("row %q at time %d: %w", update.Key, update.Time, ErrDifferentialRowNumberLagMissing)
		}
		if update.Row == nil {
			return nil, true, fmt.Errorf("row %q at time %d: %w", update.Key, update.Time, ErrDifferentialRowNumberLagRowRequired)
		}
		if update.Diff > window.maxRows || window.totalRows > window.maxRows-update.Diff {
			return nil, true, ErrDifferentialRowNumberLagMaxRows
		}
		partition = window.partition(update.Row)
	}

	before, err := window.partitionRows(window.entries, window.partitions[partition], partition)
	if err != nil {
		return nil, true, err
	}
	oldTotalRows := window.totalRows
	oldMembers, hadMembers := window.partitions[partition]

	rollback := func() {
		if exists {
			window.entries[id] = entry
		} else {
			delete(window.entries, id)
		}
		if hadMembers {
			window.partitions[partition] = oldMembers
		} else {
			delete(window.partitions, partition)
		}
		window.totalRows = oldTotalRows
	}

	if exists {
		entry.weight += update.Diff
		if entry.weight == 0 {
			delete(window.entries, id)
			members := differentialRowNumberLagRemoveID(oldMembers, id)
			if len(members) == 0 {
				delete(window.partitions, partition)
			} else {
				window.partitions[partition] = members
			}
		} else {
			window.entries[id] = entry
		}
	} else {
		entry = differentialRowNumberLagEntry{
			id:        id,
			partition: partition,
			row:       cloneDifferentialRowNumberLagSQLRow(update.Row),
			weight:    update.Diff,
		}
		window.entries[id] = entry
		window.partitions[partition] = differentialRowNumberLagInsertID(oldMembers, id)
	}
	window.totalRows += update.Diff

	after, err := window.rowsAfterSingleUpdate(before, update, exists, entry, partition)
	if err != nil {
		rollback()
		return nil, true, err
	}
	beforeByPartition := map[string][]DifferentialRowNumberLagRow{partition: before}
	afterByPartition := map[string][]DifferentialRowNumberLagRow{}
	if len(after) > 0 {
		afterByPartition[partition] = after
	}
	return differentialRowNumberLagCorrections(beforeByPartition, afterByPartition), true, nil
}

func (window *DifferentialRowNumberLagWindow) rowsAfterSingleUpdate(before []DifferentialRowNumberLagRow, update DifferentialRow, exists bool, updatedEntry differentialRowNumberLagEntry, partition string) ([]DifferentialRowNumberLagRow, error) {
	id := differentialRowNumberLagID{key: update.Key, time: update.Time}
	records := make([]differentialRowNumberLagEntry, 0, len(before)+1)
	found := false
	for index := 0; index < len(before); {
		row := before[index]
		recordID := differentialRowNumberLagID{key: row.Key, time: row.Time}
		end := index + 1
		for end < len(before) && before[end].Key == row.Key && before[end].Time == row.Time {
			end++
		}
		record := differentialRowNumberLagEntry{
			id:        recordID,
			partition: row.Partition,
			row:       row.Row,
			weight:    int64(end - index),
		}
		if exists && recordID == id {
			record.weight += update.Diff
			found = true
		}
		if record.weight > 0 {
			records = append(records, record)
		}
		index = end
	}
	if exists {
		if !found {
			return nil, ErrDifferentialRowNumberLagMissing
		}
	} else {
		insertAt := sort.Search(len(records), func(index int) bool {
			return !differentialRowNumberLagIDLess(records[index].id, id)
		})
		records = append(records, differentialRowNumberLagEntry{})
		copy(records[insertAt+1:], records[insertAt:])
		updatedEntry.partition = partition
		updatedEntry.id = id
		records[insertAt] = updatedEntry
	}
	return window.rowsFromEntries(records)
}

func (window *DifferentialRowNumberLagWindow) rowsFromEntries(records []differentialRowNumberLagEntry) ([]DifferentialRowNumberLagRow, error) {
	var totalRows int64
	for _, record := range records {
		totalRows += record.weight
	}
	rows := make([]DifferentialRowNumberLagRow, 0, minInt64ToInt(totalRows))
	var rowNumber uint64
	for _, record := range records {
		for ordinal := int64(0); ordinal < record.weight; ordinal++ {
			if rowNumber == math.MaxUint64 {
				return nil, ErrDifferentialRowNumberLagRowNumberLimit
			}
			rowNumber++
			var lagRow SQLRow
			hasLag := window.lag > 0 && len(rows) >= window.lag
			if hasLag {
				lagRow = cloneDifferentialRowNumberLagSQLRow(rows[len(rows)-window.lag].Row)
			}
			rows = append(rows, DifferentialRowNumberLagRow{
				Key:       record.id.key,
				Time:      record.id.time,
				Ordinal:   uint64(ordinal),
				Partition: record.partition,
				RowNumber: rowNumber,
				Row:       cloneDifferentialRowNumberLagSQLRow(record.row),
				HasLag:    hasLag,
				LagRow:    lagRow,
			})
		}
	}
	return rows, nil
}

func (window *DifferentialRowNumberLagWindow) applyTailFastPath(update DifferentialRow) ([]DifferentialRowNumberLagRow, bool, error) {
	if update.Diff == 0 {
		return nil, true, nil
	}
	id := differentialRowNumberLagID{key: update.Key, time: update.Time}
	entry, exists := window.entries[id]
	partition := ""
	if exists {
		partition = entry.partition
		if update.Row != nil && !reflect.DeepEqual(update.Row, entry.row) {
			return nil, true, fmt.Errorf("row %q at time %d: %w", update.Key, update.Time, ErrDifferentialRowNumberLagRowMismatch)
		}
	} else {
		if update.Diff < 0 {
			return nil, false, nil
		}
		if update.Row == nil {
			return nil, true, fmt.Errorf("row %q at time %d: %w", update.Key, update.Time, ErrDifferentialRowNumberLagRowRequired)
		}
		partition = window.partition(update.Row)
	}
	total, tail, predecessor, hasTail, hasPredecessor := window.partitionTail(partition)
	if exists {
		if !hasTail || tail.id != id {
			return nil, false, nil
		}
	} else if hasTail && !differentialRowNumberLagIDLess(tail.id, id) {
		return nil, false, nil
	}
	if update.Diff > 0 && (total > window.maxRows-update.Diff || entry.weight > math.MaxInt64-update.Diff) {
		return nil, true, ErrDifferentialRowNumberLagMaxRows
	}
	if update.Diff < 0 && (update.Diff == math.MinInt64 || !exists || entry.weight < -update.Diff) {
		return nil, true, fmt.Errorf("row %q at time %d: %w", update.Key, update.Time, ErrDifferentialRowNumberLagMissing)
	}
	if !exists && update.Diff > window.maxRows {
		return nil, true, ErrDifferentialRowNumberLagMaxRows
	}
	if exists && update.Diff < 0 {
		remove := -update.Diff
		prefix := total - entry.weight
		output := window.tailRows(entry, predecessor, hasPredecessor, prefix, entry.weight-remove, entry.weight, -1)
		entry.weight -= remove
		if entry.weight == 0 {
			delete(window.entries, id)
			members := window.partitions[partition]
			if len(members) > 0 && members[len(members)-1] == id {
				members = members[:len(members)-1]
			} else {
				members = differentialRowNumberLagRemoveID(members, id)
			}
			if len(members) == 0 {
				delete(window.partitions, partition)
			} else {
				window.partitions[partition] = members
			}
		} else {
			window.entries[id] = entry
		}
		window.totalRows -= remove
		return output, true, nil
	}
	if !exists {
		entry = differentialRowNumberLagEntry{
			id:        id,
			partition: partition,
			row:       cloneDifferentialRowNumberLagSQLRow(update.Row),
			weight:    update.Diff,
		}
		members := window.partitions[partition]
		window.partitions[partition] = append(members, id)
	} else {
		entry.weight += update.Diff
	}
	prefix := total
	if exists {
		prefix -= entry.weight - update.Diff
	}
	output := window.tailRows(entry, tail, hasTail, prefix, entry.weight-update.Diff, entry.weight, 1)
	window.entries[id] = entry
	window.totalRows += update.Diff
	return output, true, nil
}

func (window *DifferentialRowNumberLagWindow) partitionTail(partition string) (int64, differentialRowNumberLagEntry, differentialRowNumberLagEntry, bool, bool) {
	members := window.partitions[partition]
	var total int64
	var tail, predecessor differentialRowNumberLagEntry
	var hasTail, hasPredecessor bool
	for _, id := range members {
		entry, exists := window.entries[id]
		if !exists {
			continue
		}
		total += entry.weight
		if !hasTail || differentialRowNumberLagIDLess(tail.id, entry.id) {
			predecessor = tail
			hasPredecessor = hasTail
			tail = entry
			hasTail = true
		} else if !hasPredecessor || differentialRowNumberLagIDLess(predecessor.id, entry.id) {
			predecessor = entry
			hasPredecessor = true
		}
	}
	return total, tail, predecessor, hasTail, hasPredecessor
}

func differentialRowNumberLagIDLess(left, right differentialRowNumberLagID) bool {
	if left.time != right.time {
		return left.time < right.time
	}
	return left.key < right.key
}

func differentialRowNumberLagInsertID(ids []differentialRowNumberLagID, id differentialRowNumberLagID) []differentialRowNumberLagID {
	index := sort.Search(len(ids), func(index int) bool {
		return !differentialRowNumberLagIDLess(ids[index], id)
	})
	result := make([]differentialRowNumberLagID, len(ids)+1)
	copy(result, ids[:index])
	result[index] = id
	copy(result[index+1:], ids[index:])
	return result
}

func differentialRowNumberLagRemoveID(ids []differentialRowNumberLagID, id differentialRowNumberLagID) []differentialRowNumberLagID {
	index := sort.Search(len(ids), func(index int) bool {
		return !differentialRowNumberLagIDLess(ids[index], id)
	})
	if index == len(ids) || ids[index] != id {
		return ids
	}
	result := make([]differentialRowNumberLagID, len(ids)-1)
	copy(result, ids[:index])
	copy(result[index:], ids[index+1:])
	return result
}

func (window *DifferentialRowNumberLagWindow) tailRows(entry, predecessor differentialRowNumberLagEntry, hasPredecessor bool, prefix, firstOrdinal, lastOrdinal int64, diff int64) []DifferentialRowNumberLagRow {
	if lastOrdinal <= firstOrdinal {
		return nil
	}
	output := make([]DifferentialRowNumberLagRow, 0, int(lastOrdinal-firstOrdinal))
	for ordinal := firstOrdinal; ordinal < lastOrdinal; ordinal++ {
		rowIndex := prefix + ordinal
		var lagRow SQLRow
		hasLag := window.lag == 1 && rowIndex > 0
		if hasLag {
			if ordinal == 0 {
				if hasPredecessor {
					lagRow = cloneDifferentialRowNumberLagSQLRow(predecessor.row)
				}
			} else {
				lagRow = cloneDifferentialRowNumberLagSQLRow(entry.row)
			}
		}
		output = append(output, DifferentialRowNumberLagRow{
			Key:       entry.id.key,
			Time:      entry.id.time,
			Ordinal:   uint64(ordinal),
			Diff:      diff,
			Partition: entry.partition,
			RowNumber: uint64(rowIndex + 1),
			Row:       cloneDifferentialRowNumberLagSQLRow(entry.row),
			HasLag:    hasLag,
			LagRow:    lagRow,
		})
	}
	return output
}

func (window *DifferentialRowNumberLagWindow) partitionRows(entries map[differentialRowNumberLagID]differentialRowNumberLagEntry, members []differentialRowNumberLagID, partition string) ([]DifferentialRowNumberLagRow, error) {
	if len(members) == 0 {
		return nil, nil
	}
	records := make([]differentialRowNumberLagEntry, 0, len(members))
	for _, id := range members {
		if entry, exists := entries[id]; exists {
			records = append(records, entry)
		}
	}
	var partitionRows int64
	for _, record := range records {
		partitionRows += record.weight
	}
	rows := make([]DifferentialRowNumberLagRow, 0, minInt64ToInt(partitionRows))
	var rowNumber uint64
	logicalIndex := int64(0)
	for _, record := range records {
		for ordinal := int64(0); ordinal < record.weight; ordinal++ {
			if rowNumber == math.MaxUint64 {
				return nil, ErrDifferentialRowNumberLagRowNumberLimit
			}
			rowNumber++
			var lagRow SQLRow
			hasLag := window.lag > 0 && logicalIndex >= int64(window.lag)
			if hasLag {
				lagRow = cloneDifferentialRowNumberLagSQLRow(rows[logicalIndex-int64(window.lag)].Row)
			}
			rows = append(rows, DifferentialRowNumberLagRow{
				Key:       record.id.key,
				Time:      record.id.time,
				Ordinal:   uint64(ordinal),
				Diff:      1,
				Partition: partition,
				RowNumber: rowNumber,
				Row:       cloneDifferentialRowNumberLagSQLRow(record.row),
				HasLag:    hasLag,
				LagRow:    lagRow,
			})
			logicalIndex++
		}
	}
	return rows, nil
}

func differentialRowNumberLagCorrections(before map[string][]DifferentialRowNumberLagRow, after map[string][]DifferentialRowNumberLagRow) []DifferentialRowNumberLagRow {
	partitions := make([]string, 0, len(before)+len(after))
	seen := make(map[string]struct{}, len(before)+len(after))
	for partition := range before {
		seen[partition] = struct{}{}
		partitions = append(partitions, partition)
	}
	for partition := range after {
		if _, exists := seen[partition]; !exists {
			partitions = append(partitions, partition)
		}
	}
	sort.Strings(partitions)
	negative := make([]DifferentialRowNumberLagRow, 0)
	positive := make([]DifferentialRowNumberLagRow, 0)
	for _, partition := range partitions {
		oldRows := before[partition]
		newRows := after[partition]
		negative = append(negative, differentialRowNumberLagNegativeCorrections(oldRows, newRows)...)
		positive = append(positive, differentialRowNumberLagPositiveCorrections(oldRows, newRows)...)
	}
	output := make([]DifferentialRowNumberLagRow, 0, len(negative)+len(positive))
	output = append(output, negative...)
	output = append(output, positive...)
	if len(output) == 0 {
		return nil
	}
	return output
}

func differentialRowNumberLagNegativeCorrections(oldRows, newRows []DifferentialRowNumberLagRow) []DifferentialRowNumberLagRow {
	output := make([]DifferentialRowNumberLagRow, 0)
	for oldIndex, newIndex := 0, 0; oldIndex < len(oldRows); {
		if newIndex >= len(newRows) {
			row := oldRows[oldIndex]
			row.Diff = -1
			output = append(output, row)
			oldIndex++
			continue
		}
		oldRow, newRow := oldRows[oldIndex], newRows[newIndex]
		oldID := differentialRowNumberLagOutputID{partition: oldRow.Partition, differentialRowNumberLagID: differentialRowNumberLagID{key: oldRow.Key, time: oldRow.Time}, ordinal: oldRow.Ordinal}
		newID := differentialRowNumberLagOutputID{partition: newRow.Partition, differentialRowNumberLagID: differentialRowNumberLagID{key: newRow.Key, time: newRow.Time}, ordinal: newRow.Ordinal}
		if oldID == newID {
			if !differentialRowNumberLagEqual(oldRow, newRow) {
				oldRow.Diff = -1
				output = append(output, oldRow)
			}
			oldIndex++
			newIndex++
			continue
		}
		if differentialRowNumberLagOutputIDLess(oldID, newID) {
			oldRow.Diff = -1
			output = append(output, oldRow)
			oldIndex++
			continue
		}
		newIndex++
	}
	return output
}

func differentialRowNumberLagPositiveCorrections(oldRows, newRows []DifferentialRowNumberLagRow) []DifferentialRowNumberLagRow {
	output := make([]DifferentialRowNumberLagRow, 0)
	for oldIndex, newIndex := 0, 0; newIndex < len(newRows); {
		if oldIndex >= len(oldRows) {
			row := newRows[newIndex]
			row.Diff = 1
			output = append(output, row)
			newIndex++
			continue
		}
		oldRow, newRow := oldRows[oldIndex], newRows[newIndex]
		oldID := differentialRowNumberLagOutputID{partition: oldRow.Partition, differentialRowNumberLagID: differentialRowNumberLagID{key: oldRow.Key, time: oldRow.Time}, ordinal: oldRow.Ordinal}
		newID := differentialRowNumberLagOutputID{partition: newRow.Partition, differentialRowNumberLagID: differentialRowNumberLagID{key: newRow.Key, time: newRow.Time}, ordinal: newRow.Ordinal}
		if oldID == newID {
			if !differentialRowNumberLagEqual(oldRow, newRow) {
				newRow.Diff = 1
				output = append(output, newRow)
			}
			oldIndex++
			newIndex++
			continue
		}
		if differentialRowNumberLagOutputIDLess(oldID, newID) {
			oldIndex++
			continue
		}
		newRow.Diff = 1
		output = append(output, newRow)
		newIndex++
	}
	return output
}

func differentialRowNumberLagOutputIDLess(left, right differentialRowNumberLagOutputID) bool {
	if left.partition != right.partition {
		return left.partition < right.partition
	}
	if left.time != right.time {
		return left.time < right.time
	}
	if left.key != right.key {
		return left.key < right.key
	}
	return left.ordinal < right.ordinal
}

func differentialRowNumberLagEqual(left, right DifferentialRowNumberLagRow) bool {
	return left.Partition == right.Partition && left.Key == right.Key && left.Time == right.Time && left.Ordinal == right.Ordinal && left.RowNumber == right.RowNumber && reflect.DeepEqual(left.Row, right.Row) && left.HasLag == right.HasLag && reflect.DeepEqual(left.LagRow, right.LagRow)
}

func minInt64ToInt(value int64) int {
	if value <= 0 {
		return 0
	}
	if uint64(value) > uint64(int(^uint(0)>>1)) {
		return int(^uint(0) >> 1)
	}
	return int(value)
}

func cloneDifferentialRowNumberLagSQLRow(row SQLRow) SQLRow {
	if row == nil {
		return nil
	}
	clone := make(Row, len(row))
	for key, value := range row {
		clone[key] = cloneDifferentialRowNumberLagValue(value)
	}
	return clone
}

func cloneDifferentialRowNumberLagValue(value interface{}) interface{} {
	switch value := value.(type) {
	case []byte:
		return append([]byte(nil), value...)
	case Row:
		return cloneDifferentialRowNumberLagSQLRow(value)
	case map[string]interface{}:
		return cloneDifferentialRowNumberLagSQLRow(Row(value))
	case []interface{}:
		clone := make([]interface{}, len(value))
		for index, item := range value {
			clone[index] = cloneDifferentialRowNumberLagValue(item)
		}
		return clone
	default:
		return value
	}
}
