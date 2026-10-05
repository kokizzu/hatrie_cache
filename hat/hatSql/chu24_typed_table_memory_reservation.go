package hatSql

import (
	"errors"
	"fmt"
)

// ErrTypedTableMemoryReservationInvalid reports an invalid working-memory
// reservation request.
var ErrTypedTableMemoryReservationInvalid = errors.New("typed table memory reservation is invalid")

// TypedTableMemoryReservation owns a bounded working-memory reservation. A
// reservation is released explicitly and Release is safe to call repeatedly.
type TypedTableMemoryReservation struct {
	table    *TypedTable
	bytes    int64
	released bool
}

// ReserveMemory reserves caller-owned working memory against the table budget.
// It is intended for index, query, or arrangement memory that is not part of
// the table's row estimate. A disabled budget returns a nil reservation and
// does not add synchronization or allocation to the caller's path.
func (table *TypedTable) ReserveMemory(bytes int64) (*TypedTableMemoryReservation, error) {
	if table == nil {
		return nil, fmt.Errorf("typed table is nil")
	}
	if bytes < 0 {
		return nil, fmt.Errorf("%w: bytes must be non-negative", ErrTypedTableMemoryReservationInvalid)
	}
	if bytes == 0 {
		return nil, nil
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	if table.memoryBudgetMaxBytes <= 0 {
		return nil, nil
	}
	current := typedTableAddMemoryBytes(table.memoryBytes, table.memoryReservedBytes)
	if current > table.memoryBudgetMaxBytes || bytes > table.memoryBudgetMaxBytes-current {
		return nil, fmt.Errorf("%w: maximum %d bytes, current %d bytes, requested reservation %d bytes", ErrTypedTableMemoryBudgetExceeded, table.memoryBudgetMaxBytes, current, bytes)
	}
	table.memoryReservedBytes += bytes
	return &TypedTableMemoryReservation{table: table, bytes: bytes}, nil
}

// Release returns the reservation to the table budget. It is idempotent so a
// deferred release can safely coexist with an explicit cleanup path.
func (reservation *TypedTableMemoryReservation) Release() {
	if reservation == nil || reservation.table == nil {
		return
	}
	table := reservation.table
	table.mu.Lock()
	defer table.mu.Unlock()
	if reservation.released {
		return
	}
	reservation.released = true
	if reservation.bytes >= table.memoryReservedBytes {
		table.memoryReservedBytes = 0
		return
	}
	table.memoryReservedBytes -= reservation.bytes
}
