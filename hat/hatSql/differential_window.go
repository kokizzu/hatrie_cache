package hatSql

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"sync"
)

const DefaultDifferentialWindowMaxRows = 1_000_000

var (
	ErrDifferentialWindowNil               = errors.New("hatSql: differential window is nil")
	ErrDifferentialWindowFrameInvalid      = errors.New("hatSql: differential window frame is invalid")
	ErrDifferentialWindowMaxRows           = errors.New("hatSql: differential window row limit exceeded")
	ErrDifferentialWindowMaxRowsInvalid    = errors.New("hatSql: differential window row limit is invalid")
	ErrDifferentialWindowRetractionMissing = errors.New("hatSql: differential window retraction is missing")
	ErrDifferentialWindowRowMismatch       = errors.New("hatSql: differential window row identity changed")
	ErrDifferentialWindowWeightOverflow    = errors.New("hatSql: differential window weight overflowed")
	ErrDifferentialWindowValueRequired     = errors.New("hatSql: differential window insert requires a row")
)

// DifferentialWindowFrameMode selects the ordering semantics for a frame.
// Rows uses logical differential records as ordered rows. Range includes all
// records whose Time is within the signed Start..End interval around a row.
type DifferentialWindowFrameMode uint8

const (
	DifferentialWindowFrameRows DifferentialWindowFrameMode = iota + 1
	DifferentialWindowFrameRange
)

// DifferentialWindowValue extracts an optional numeric value for frame sums.
// Returning false excludes the row from FrameSum while it remains in
// FrameCount. A nil extractor disables sum calculation.
type DifferentialWindowValue func(SQLRow) (float64, bool, error)

// DifferentialWindowOptions configures an exact differential window. Start
// and End are inclusive signed frame offsets. For ROWS, -1..0 means one
// preceding logical record through the current record. For RANGE, -10..0
// means timestamps from ten units before the current row through that row.
type DifferentialWindowOptions struct {
	PartitionKey func(SQLRow) string
	Mode         DifferentialWindowFrameMode
	Start        int64
	End          int64
	Value        DifferentialWindowValue
	MaxRows      int
}

// DifferentialWindowRow is one weighted current or corrected window result.
// Apply returns negative rows retracting old results and positive rows adding
// new results. Snapshot returns only the current positive state.
type DifferentialWindowRow struct {
	Key         string
	Time        uint64
	Diff        int64
	Partition   string
	Row         SQLRow
	FrameCount  int64
	FrameSum    float64
	HasFrameSum bool
}

type differentialWindowRowID struct {
	key  string
	time uint64
}

type differentialWindowEntry struct {
	id        differentialWindowRowID
	partition string
	row       SQLRow
	weight    int64
}

type differentialWindowOutputID struct {
	partition string
	differentialWindowRowID
}

// DifferentialWindow maintains exact ROWS or RANGE frame count/sum results
// from signed differential updates. It is intentionally opt-in and keeps the
// existing append-only IncrementalRowNumberLagWindow behavior unchanged.
type DifferentialWindow struct {
	mu           sync.RWMutex
	partitionKey func(SQLRow) string
	mode         DifferentialWindowFrameMode
	start        int64
	end          int64
	value        DifferentialWindowValue
	maxRows      int
	entries      map[differentialWindowRowID]differentialWindowEntry
	partitions   map[string]map[differentialWindowRowID]struct{}
}

// NewDifferentialWindow validates options and creates an empty exact window.
func NewDifferentialWindow(options DifferentialWindowOptions) (*DifferentialWindow, error) {
	if options.Mode == 0 {
		options.Mode = DifferentialWindowFrameRows
	}
	if options.Mode != DifferentialWindowFrameRows && options.Mode != DifferentialWindowFrameRange {
		return nil, ErrDifferentialWindowFrameInvalid
	}
	if options.Start > options.End {
		return nil, ErrDifferentialWindowFrameInvalid
	}
	if options.MaxRows < 0 {
		return nil, ErrDifferentialWindowMaxRowsInvalid
	}
	if options.MaxRows == 0 {
		options.MaxRows = DefaultDifferentialWindowMaxRows
	}
	return &DifferentialWindow{
		partitionKey: options.PartitionKey,
		mode:         options.Mode,
		start:        options.Start,
		end:          options.End,
		value:        options.Value,
		maxRows:      options.MaxRows,
		entries:      make(map[differentialWindowRowID]differentialWindowEntry),
		partitions:   make(map[string]map[differentialWindowRowID]struct{}),
	}, nil
}

// Apply applies a batch atomically and returns the differential corrections
// caused by it. Negative updates may arrive out of order and may remove part
// of a weighted record. Invalid input leaves the previous state unchanged.
func (window *DifferentialWindow) Apply(updates []DifferentialRow) ([]DifferentialWindowRow, error) {
	if window == nil {
		return nil, ErrDifferentialWindowNil
	}
	if len(updates) == 0 {
		return nil, nil
	}
	window.mu.Lock()
	defer window.mu.Unlock()

	workingEntries := make(map[differentialWindowRowID]differentialWindowEntry, len(window.entries)+len(updates))
	for id, entry := range window.entries {
		workingEntries[id] = entry
	}
	workingPartitions := make(map[string]map[differentialWindowRowID]struct{}, len(window.partitions))
	for partition, members := range window.partitions {
		workingPartitions[partition] = members
	}
	mutablePartitions := make(map[string]bool)
	affected := make(map[string]struct{})
	before := make(map[string][]DifferentialWindowRow)
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
	mutablePartition := func(partition string) map[differentialWindowRowID]struct{} {
		members, exists := workingPartitions[partition]
		if !exists {
			members = make(map[differentialWindowRowID]struct{})
			workingPartitions[partition] = members
			mutablePartitions[partition] = true
			return members
		}
		if !mutablePartitions[partition] {
			clone := make(map[differentialWindowRowID]struct{}, len(members)+1)
			for id := range members {
				clone[id] = struct{}{}
			}
			members = clone
			workingPartitions[partition] = members
			mutablePartitions[partition] = true
		}
		return members
	}

	for _, update := range updates {
		if update.Diff == 0 {
			continue
		}
		id := differentialWindowRowID{key: update.Key, time: update.Time}
		entry, exists := workingEntries[id]
		partition := ""
		if exists {
			partition = entry.partition
			if update.Row != nil && !reflect.DeepEqual(update.Row, entry.row) {
				return nil, fmt.Errorf("row %q at time %d: %w", update.Key, update.Time, ErrDifferentialWindowRowMismatch)
			}
		} else {
			if update.Diff < 0 {
				return nil, fmt.Errorf("row %q at time %d: %w", update.Key, update.Time, ErrDifferentialWindowRetractionMissing)
			}
			if update.Row == nil {
				return nil, fmt.Errorf("row %q at time %d: %w", update.Key, update.Time, ErrDifferentialWindowValueRequired)
			}
			partition = window.partition(update.Row)
		}
		if err := ensureBefore(partition); err != nil {
			return nil, err
		}
		affected[partition] = struct{}{}

		if exists {
			if update.Diff > 0 && entry.weight > math.MaxInt64-update.Diff {
				return nil, fmt.Errorf("row %q at time %d: %w", update.Key, update.Time, ErrDifferentialWindowWeightOverflow)
			}
			if update.Diff < 0 && (update.Diff == -1<<63 || entry.weight < -update.Diff) {
				return nil, fmt.Errorf("row %q at time %d: %w", update.Key, update.Time, ErrDifferentialWindowRetractionMissing)
			}
			entry.weight += update.Diff
			if entry.weight == 0 {
				delete(workingEntries, id)
				members := mutablePartition(partition)
				delete(members, id)
				if len(members) == 0 {
					delete(workingPartitions, partition)
				}
				continue
			}
			workingEntries[id] = entry
			continue
		}

		if len(workingEntries) >= window.maxRows {
			return nil, ErrDifferentialWindowMaxRows
		}
		entry = differentialWindowEntry{
			id:        id,
			partition: partition,
			row:       cloneDifferentialWindowRow(update.Row),
			weight:    update.Diff,
		}
		workingEntries[id] = entry
		mutablePartition(partition)[id] = struct{}{}
	}

	after := make(map[string][]DifferentialWindowRow, len(affected))
	for partition := range affected {
		rows, err := window.partitionRows(workingEntries, workingPartitions[partition], partition)
		if err != nil {
			return nil, err
		}
		after[partition] = rows
	}
	output := differentialWindowCorrections(before, after)
	window.entries = workingEntries
	window.partitions = workingPartitions
	return output, nil
}

// Snapshot returns all current window results in deterministic order.
func (window *DifferentialWindow) Snapshot() []DifferentialWindowRow {
	rows, err := window.SnapshotWithError()
	if err != nil {
		return nil
	}
	return rows
}

// SnapshotWithError returns all current window results and reports a value
// callback error instead of silently returning an empty snapshot.
func (window *DifferentialWindow) SnapshotWithError() ([]DifferentialWindowRow, error) {
	if window == nil {
		return nil, ErrDifferentialWindowNil
	}
	window.mu.RLock()
	defer window.mu.RUnlock()
	partitions := make([]string, 0, len(window.partitions))
	for partition := range window.partitions {
		partitions = append(partitions, partition)
	}
	sort.Strings(partitions)
	output := make([]DifferentialWindowRow, 0, len(window.entries))
	for _, partition := range partitions {
		rows, err := window.partitionRows(window.entries, window.partitions[partition], partition)
		if err != nil {
			return nil, err
		}
		output = append(output, rows...)
	}
	if len(output) == 0 {
		return nil, nil
	}
	return output, nil
}

// Len returns the number of distinct logical records retained by the window.
func (window *DifferentialWindow) Len() int {
	if window == nil {
		return 0
	}
	window.mu.RLock()
	defer window.mu.RUnlock()
	return len(window.entries)
}

func (window *DifferentialWindow) partition(row SQLRow) string {
	if window.partitionKey == nil {
		return ""
	}
	return window.partitionKey(row)
}

func (window *DifferentialWindow) partitionRows(entries map[differentialWindowRowID]differentialWindowEntry, members map[differentialWindowRowID]struct{}, partition string) ([]DifferentialWindowRow, error) {
	if len(members) == 0 {
		return nil, nil
	}
	records := make([]differentialWindowEntry, 0, len(members))
	for id := range members {
		entry, exists := entries[id]
		if exists {
			records = append(records, entry)
		}
	}
	sort.Slice(records, func(left, right int) bool {
		if records[left].id.time != records[right].id.time {
			return records[left].id.time < records[right].id.time
		}
		return records[left].id.key < records[right].id.key
	})
	prefixCounts := make([]int64, len(records)+1)
	prefixSums := make([]float64, len(records)+1)
	if window.value != nil {
		for index, record := range records {
			value, valid, err := window.value(cloneDifferentialWindowRow(record.row))
			if err != nil {
				return nil, fmt.Errorf("row %q at time %d: %w", record.id.key, record.id.time, err)
			}
			if valid {
				prefixSums[index+1] = prefixSums[index] + float64(record.weight)*value
			} else {
				prefixSums[index+1] = prefixSums[index]
			}
		}
	}
	for index, record := range records {
		if record.weight > math.MaxInt64-prefixCounts[index] {
			return nil, ErrDifferentialWindowWeightOverflow
		}
		prefixCounts[index+1] = prefixCounts[index] + record.weight
	}
	output := make([]DifferentialWindowRow, 0, len(records))
	for index, record := range records {
		start, end := window.frameIndexes(records, index)
		var frameCount int64
		var frameSum float64
		if start <= end {
			frameCount = prefixCounts[end+1] - prefixCounts[start]
			frameSum = prefixSums[end+1] - prefixSums[start]
		}
		output = append(output, DifferentialWindowRow{
			Key:         record.id.key,
			Time:        record.id.time,
			Diff:        record.weight,
			Partition:   partition,
			Row:         cloneDifferentialWindowRow(record.row),
			FrameCount:  frameCount,
			FrameSum:    frameSum,
			HasFrameSum: window.value != nil,
		})
	}
	return output, nil
}

func (window *DifferentialWindow) frameIndexes(records []differentialWindowEntry, index int) (int, int) {
	if window.mode == DifferentialWindowFrameRows {
		start := differentialWindowIndexOffset(index, window.start)
		end := differentialWindowIndexOffset(index, window.end)
		if start < 0 {
			start = 0
		}
		if end >= int64(len(records)) {
			end = int64(len(records) - 1)
		}
		if start >= int64(len(records)) {
			start = int64(len(records))
		}
		if end < -1 {
			end = -1
		}
		return int(start), int(end)
	}
	lower := differentialWindowShift(records[index].id.time, window.start)
	upper := differentialWindowShift(records[index].id.time, window.end)
	start := sort.Search(len(records), func(recordIndex int) bool {
		return records[recordIndex].id.time >= lower
	})
	endExclusive := sort.Search(len(records), func(recordIndex int) bool {
		return records[recordIndex].id.time > upper
	})
	return start, endExclusive - 1
}

func differentialWindowIndexOffset(index int, offset int64) int64 {
	base := int64(index)
	const minInt64 = -1 << 63
	const maxInt64 = 1<<63 - 1
	if offset > 0 && base > maxInt64-offset {
		return maxInt64
	}
	if offset < 0 && base < minInt64-offset {
		return minInt64
	}
	return base + offset
}

func differentialWindowShift(value uint64, offset int64) uint64 {
	if offset >= 0 {
		delta := uint64(offset)
		if ^uint64(0)-value < delta {
			return ^uint64(0)
		}
		return value + delta
	}
	delta := uint64(-(offset + 1)) + 1
	if delta > value {
		return 0
	}
	return value - delta
}

func differentialWindowCorrections(before, after map[string][]DifferentialWindowRow) []DifferentialWindowRow {
	oldRows := make(map[differentialWindowOutputID]DifferentialWindowRow)
	newRows := make(map[differentialWindowOutputID]DifferentialWindowRow)
	for partition, rows := range before {
		for _, row := range rows {
			oldRows[differentialWindowOutputID{partition: partition, differentialWindowRowID: differentialWindowRowID{key: row.Key, time: row.Time}}] = row
		}
	}
	for partition, rows := range after {
		for _, row := range rows {
			newRows[differentialWindowOutputID{partition: partition, differentialWindowRowID: differentialWindowRowID{key: row.Key, time: row.Time}}] = row
		}
	}
	ids := make([]differentialWindowOutputID, 0, len(oldRows)+len(newRows))
	seen := make(map[differentialWindowOutputID]struct{}, len(oldRows)+len(newRows))
	for id := range oldRows {
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	for id := range newRows {
		if _, exists := seen[id]; !exists {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(left, right int) bool {
		if ids[left].partition != ids[right].partition {
			return ids[left].partition < ids[right].partition
		}
		if ids[left].time != ids[right].time {
			return ids[left].time < ids[right].time
		}
		return ids[left].key < ids[right].key
	})
	output := make([]DifferentialWindowRow, 0, len(ids)*2)
	for _, id := range ids {
		oldRow, hadOld := oldRows[id]
		newRow, hasNew := newRows[id]
		if hadOld && (!hasNew || !differentialWindowRowsEqual(oldRow, newRow)) {
			oldRow.Diff = -oldRow.Diff
			output = append(output, oldRow)
		}
		if hasNew && (!hadOld || !differentialWindowRowsEqual(oldRow, newRow)) {
			output = append(output, newRow)
		}
	}
	return output
}

func differentialWindowRowsEqual(left, right DifferentialWindowRow) bool {
	return left.Key == right.Key && left.Time == right.Time && left.Diff == right.Diff && left.Partition == right.Partition &&
		left.FrameCount == right.FrameCount && left.HasFrameSum == right.HasFrameSum &&
		math.Float64bits(left.FrameSum) == math.Float64bits(right.FrameSum) && reflect.DeepEqual(left.Row, right.Row)
}

func cloneDifferentialWindowRow(row SQLRow) SQLRow {
	if row == nil {
		return nil
	}
	clone := make(Row, len(row))
	for key, value := range row {
		clone[key] = cloneDifferentialWindowValue(value)
	}
	return clone
}

func cloneDifferentialWindowValue(value interface{}) interface{} {
	switch value := value.(type) {
	case []byte:
		return append([]byte(nil), value...)
	case Row:
		return cloneDifferentialWindowRow(value)
	case map[string]interface{}:
		return cloneDifferentialWindowRow(Row(value))
	case []interface{}:
		clone := make([]interface{}, len(value))
		for index, item := range value {
			clone[index] = cloneDifferentialWindowValue(item)
		}
		return clone
	default:
		return value
	}
}
