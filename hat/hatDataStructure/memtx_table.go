package hatDataStructure

import (
	"errors"
	"sync"
)

const (
	// DefaultMemtxTableCapacity is used when no capacity is supplied.
	DefaultMemtxTableCapacity = 1024
	// MaxMemtxTableCapacity prevents accidental oversized preallocation.
	MaxMemtxTableCapacity = 1 << 24
)

var (
	ErrMemtxTableNil             = errors.New("memtx table is nil")
	ErrMemtxTableCapacityInvalid = errors.New("memtx table capacity is invalid")
	ErrMemtxTableDuplicateID     = errors.New("memtx table id already exists")
	ErrMemtxTableFull            = errors.New("memtx table is full")
)

// MemtxTableOptions controls construction of a fixed-capacity in-memory table.
type MemtxTableOptions struct {
	Capacity int
}

// MemtxEntry is a row returned by MemtxTable.ScanInto.
type MemtxEntry[T any] struct {
	ID    uint64
	Value T
}

type memtxSlot[T any] struct {
	id       uint64
	value    T
	occupied bool
}

// MemtxTable is an opt-in fixed-capacity row table keyed by uint64 IDs.
//
// Rows are stored in preallocated slots. Lookups use an ID-to-slot index, while
// scans walk slots in physical order and can reuse caller-owned output storage.
// The table is safe for concurrent access.
type MemtxTable[T any] struct {
	mu        sync.RWMutex
	slots     []memtxSlot[T]
	positions map[uint64]uint32
	free      []uint32
	next      uint32
	live      int
}

// NewMemtxTable creates a fixed-capacity table. A zero capacity selects the
// package default.
func NewMemtxTable[T any](options MemtxTableOptions) (*MemtxTable[T], error) {
	capacity := options.Capacity
	if capacity == 0 {
		capacity = DefaultMemtxTableCapacity
	}
	if capacity < 1 || capacity > MaxMemtxTableCapacity {
		return nil, ErrMemtxTableCapacityInvalid
	}
	return &MemtxTable[T]{
		slots:     make([]memtxSlot[T], capacity),
		positions: make(map[uint64]uint32, capacity),
		free:      make([]uint32, 0, capacity),
	}, nil
}

// Insert adds a new row and rejects duplicate IDs.
func (table *MemtxTable[T]) Insert(id uint64, value T) error {
	if table == nil {
		return ErrMemtxTableNil
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	if _, exists := table.positions[id]; exists {
		return ErrMemtxTableDuplicateID
	}
	return table.insertLocked(id, value)
}

// Upsert inserts a row or updates the existing row. The returned bool is true
// only when a new row was inserted.
func (table *MemtxTable[T]) Upsert(id uint64, value T) (inserted bool, err error) {
	if table == nil {
		return false, ErrMemtxTableNil
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	if position, exists := table.positions[id]; exists {
		table.slots[position].value = value
		return false, nil
	}
	if err := table.insertLocked(id, value); err != nil {
		return false, err
	}
	return true, nil
}

// Get returns a row by ID.
func (table *MemtxTable[T]) Get(id uint64) (value T, ok bool) {
	if table == nil {
		return value, false
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	position, exists := table.positions[id]
	if !exists {
		return value, false
	}
	return table.slots[position].value, true
}

// Delete removes a row by ID and reports whether it existed.
func (table *MemtxTable[T]) Delete(id uint64) bool {
	if table == nil {
		return false
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	position, exists := table.positions[id]
	if !exists {
		return false
	}
	delete(table.positions, id)
	table.slots[position] = memtxSlot[T]{}
	table.free = append(table.free, position)
	table.live--
	return true
}

// Len returns the number of live rows.
func (table *MemtxTable[T]) Len() int {
	if table == nil {
		return 0
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	return table.live
}

// Capacity returns the configured row capacity.
func (table *MemtxTable[T]) Capacity() int {
	if table == nil {
		return 0
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	return len(table.slots)
}

// ScanInto appends live rows to dst in physical slot order. It reuses dst's
// backing array when it has enough capacity.
func (table *MemtxTable[T]) ScanInto(dst []MemtxEntry[T]) []MemtxEntry[T] {
	if table == nil {
		return dst[:0]
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	if cap(dst) < table.live {
		dst = make([]MemtxEntry[T], 0, table.live)
	} else {
		dst = dst[:0]
	}
	for _, slot := range table.slots {
		if slot.occupied {
			dst = append(dst, MemtxEntry[T]{ID: slot.id, Value: slot.value})
		}
	}
	return dst
}

// Reset removes all rows while retaining the table's allocated storage.
func (table *MemtxTable[T]) Reset() {
	if table == nil {
		return
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	for index := range table.slots {
		table.slots[index] = memtxSlot[T]{}
	}
	for id := range table.positions {
		delete(table.positions, id)
	}
	table.free = table.free[:0]
	table.next = 0
	table.live = 0
}

func (table *MemtxTable[T]) insertLocked(id uint64, value T) error {
	position, ok := table.acquireSlotLocked()
	if !ok {
		return ErrMemtxTableFull
	}
	table.slots[position] = memtxSlot[T]{id: id, value: value, occupied: true}
	table.positions[id] = position
	table.live++
	return nil
}

func (table *MemtxTable[T]) acquireSlotLocked() (uint32, bool) {
	if free := len(table.free); free > 0 {
		position := table.free[free-1]
		table.free = table.free[:free-1]
		return position, true
	}
	if table.next >= uint32(len(table.slots)) {
		return 0, false
	}
	position := table.next
	table.next++
	return position, true
}
