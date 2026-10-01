package hatDataStructure

import (
	"errors"
	"sync"
)

var (
	// ErrMemtxRowTableColumnCountInvalid reports a negative fixed tuple width.
	ErrMemtxRowTableColumnCountInvalid = errors.New("hatriecache: memtx row table column count is invalid")
	// ErrMemtxRowTableCapacityInvalid reports a negative initial row capacity.
	ErrMemtxRowTableCapacityInvalid = errors.New("hatriecache: memtx row table capacity is invalid")
	// ErrMemtxRowTableNil reports an operation on a nil table.
	ErrMemtxRowTableNil = errors.New("hatriecache: memtx row table is nil")
	// ErrMemtxRowTableKeyInvalid reports an empty tuple key.
	ErrMemtxRowTableKeyInvalid = errors.New("hatriecache: memtx row table key is empty")
	// ErrMemtxRowTableWidthMismatch reports a tuple with the wrong number of values.
	ErrMemtxRowTableWidthMismatch = errors.New("hatriecache: memtx row table tuple width mismatch")
)

// MemtxRowTable is a bounded-width in-memory tuple table inspired by
// Tarantool's memtx engine. Keys use one index map, while tuple values share a
// row-major backing array instead of allocating one map or slice per row.
//
// The table is safe for concurrent readers and writers. Get returns a detached
// copy. Visit is also detached and may call back into the table; use
// VisitBorrowed for allocation-free scans when the callback does not retain or
// mutate the values and does not call back into the table.
//
// Deletes leave physical slots until Compact is called. This keeps mutation
// paths predictable and makes compaction an explicit operator choice.
type MemtxRowTable struct {
	mu           sync.RWMutex
	columnCount  int
	keys         []string
	values       []any
	live         []bool
	index        map[string]int
	liveRowCount int
}

// NewMemtxRowTable creates a fixed-width table with no preallocated rows.
// A zero column count is valid for key-only tuples.
func NewMemtxRowTable(columnCount int) (*MemtxRowTable, error) {
	return NewMemtxRowTableWithCapacity(columnCount, 0)
}

// NewMemtxRowTableWithCapacity creates a fixed-width table with room for
// capacity physical rows. Capacity only affects initial backing allocation.
func NewMemtxRowTableWithCapacity(columnCount, capacity int) (*MemtxRowTable, error) {
	if columnCount < 0 {
		return nil, ErrMemtxRowTableColumnCountInvalid
	}
	if capacity < 0 {
		return nil, ErrMemtxRowTableCapacityInvalid
	}
	valueCapacity := 0
	if columnCount != 0 {
		maxInt := int(^uint(0) >> 1)
		if capacity > maxInt/columnCount {
			return nil, ErrMemtxRowTableCapacityInvalid
		}
		valueCapacity = capacity * columnCount
	}
	return &MemtxRowTable{
		columnCount: columnCount,
		keys:        make([]string, 0, capacity),
		values:      make([]any, 0, valueCapacity),
		live:        make([]bool, 0, capacity),
		index:       make(map[string]int, capacity),
	}, nil
}

// ColumnCount returns the fixed tuple width.
func (table *MemtxRowTable) ColumnCount() int {
	if table == nil {
		return 0
	}
	return table.columnCount
}

// Upsert inserts or replaces one tuple. The input values are copied into the
// table's shared backing array.
func (table *MemtxRowTable) Upsert(key string, values []any) error {
	if table == nil {
		return ErrMemtxRowTableNil
	}
	if key == "" {
		return ErrMemtxRowTableKeyInvalid
	}
	if len(values) != table.columnCount {
		return ErrMemtxRowTableWidthMismatch
	}

	table.mu.Lock()
	defer table.mu.Unlock()
	if slot, exists := table.index[key]; exists {
		start := slot * table.columnCount
		copy(table.values[start:start+table.columnCount], values)
		return nil
	}
	if table.index == nil {
		table.index = make(map[string]int)
	}
	slot := len(table.keys)
	table.keys = append(table.keys, key)
	table.live = append(table.live, true)
	table.values = append(table.values, values...)
	table.index[key] = slot
	table.liveRowCount++
	return nil
}

// Get returns a detached copy of the tuple for key.
func (table *MemtxRowTable) Get(key string) ([]any, bool) {
	if table == nil {
		return nil, false
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	slot, ok := table.index[key]
	if !ok || !table.live[slot] {
		return nil, false
	}
	values := make([]any, table.columnCount)
	copy(values, table.values[slot*table.columnCount:(slot+1)*table.columnCount])
	return values, true
}

// GetInto copies a tuple into dst and reuses its backing array when possible.
// A missing key returns dst[:0], false.
func (table *MemtxRowTable) GetInto(dst []any, key string) ([]any, bool) {
	if table == nil {
		return dst[:0], false
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	slot, ok := table.index[key]
	if !ok || !table.live[slot] {
		return dst[:0], false
	}
	if cap(dst) < table.columnCount {
		dst = make([]any, table.columnCount)
	} else {
		dst = dst[:table.columnCount]
	}
	copy(dst, table.values[slot*table.columnCount:(slot+1)*table.columnCount])
	return dst, true
}

// Delete removes key and releases references held by its physical slot.
// Physical storage is reclaimed by Compact.
func (table *MemtxRowTable) Delete(key string) bool {
	if table == nil {
		return false
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	slot, ok := table.index[key]
	if !ok || !table.live[slot] {
		return false
	}
	delete(table.index, key)
	table.keys[slot] = ""
	table.live[slot] = false
	clear(table.values[slot*table.columnCount : (slot+1)*table.columnCount])
	table.liveRowCount--
	return true
}

// Visit calls fn with detached rows in insertion order. Returning false stops
// the visit. A nil callback is a no-op.
func (table *MemtxRowTable) Visit(fn func(key string, values []any) bool) {
	if table == nil || fn == nil {
		return
	}
	table.mu.RLock()
	rows := make([]memtxRowTableSnapshot, 0, table.liveRowCount)
	for slot, key := range table.keys {
		if !table.live[slot] {
			continue
		}
		values := make([]any, table.columnCount)
		copy(values, table.values[slot*table.columnCount:(slot+1)*table.columnCount])
		rows = append(rows, memtxRowTableSnapshot{key: key, values: values})
	}
	table.mu.RUnlock()
	for _, row := range rows {
		if !fn(row.key, row.values) {
			return
		}
	}
}

// VisitBorrowed scans live rows without per-row allocations. Values is a
// read-only view valid only during the callback; retaining or mutating it is
// invalid. The callback must not call back into table.
func (table *MemtxRowTable) VisitBorrowed(fn func(key string, values []any) bool) {
	if table == nil || fn == nil {
		return
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	for slot, key := range table.keys {
		if !table.live[slot] {
			continue
		}
		if !fn(key, table.values[slot*table.columnCount:(slot+1)*table.columnCount]) {
			return
		}
	}
}

// Compact removes deleted physical slots while preserving insertion order.
func (table *MemtxRowTable) Compact() {
	if table == nil {
		return
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	if table.liveRowCount == len(table.keys) {
		return
	}
	keys := make([]string, 0, table.liveRowCount)
	values := make([]any, 0, table.liveRowCount*table.columnCount)
	live := make([]bool, 0, table.liveRowCount)
	index := make(map[string]int, table.liveRowCount)
	for slot, key := range table.keys {
		if !table.live[slot] {
			continue
		}
		newSlot := len(keys)
		keys = append(keys, key)
		live = append(live, true)
		values = append(values, table.values[slot*table.columnCount:(slot+1)*table.columnCount]...)
		index[key] = newSlot
	}
	table.keys = keys
	table.values = values
	table.live = live
	table.index = index
}

// Len returns the number of live tuples.
func (table *MemtxRowTable) Len() int {
	if table == nil {
		return 0
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	return table.liveRowCount
}

// PhysicalRows returns live and deleted physical slots before compaction.
func (table *MemtxRowTable) PhysicalRows() int {
	if table == nil {
		return 0
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	return len(table.keys)
}

type memtxRowTableSnapshot struct {
	key    string
	values []any
}
