package hatSql

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// TypedTableTTLMode selects the timestamp used to determine row expiry.
type TypedTableTTLMode uint8

const (
	// TypedTableTTLDisabled leaves row lifetime unchanged.
	TypedTableTTLDisabled TypedTableTTLMode = iota
	// TypedTableTTLProcessingTime starts the lifetime when Upsert accepts a row.
	TypedTableTTLProcessingTime
	// TypedTableTTLEventTime derives the lifetime from an int64 Unix-nanosecond column.
	TypedTableTTLEventTime
)

// TypedTableTTLOptions configures opt-in row expiration. Processing-time TTL
// retains one int64 deadline per physical row. Event-time TTL derives the
// deadline from Field and retains no additional per-row timestamp state.
// Clock is used only for processing-time inserts and SQL visibility checks;
// callers can provide it to make expiry deterministic in tests.
type TypedTableTTLOptions struct {
	Mode     TypedTableTTLMode
	Field    string
	Lifetime time.Duration
	Clock    func() time.Time
}

type typedTableTTLState struct {
	options         TypedTableTTLOptions
	field           int
	deadlines       []int64
	expiryHeap      []typedTableTTLExpiryEntry
	expiryPositions []int
}

type typedTableColumnTTLState struct {
	options   TypedTableTTLOptions
	field     int
	deadlines []int64
}

func newTypedTableTTLState(options TypedTableTTLOptions, columns []TypedTableColumn, byName map[string]int) (*typedTableTTLState, error) {
	options.Field = strings.TrimSpace(options.Field)
	switch options.Mode {
	case TypedTableTTLDisabled:
		return nil, nil
	case TypedTableTTLProcessingTime:
		if options.Field != "" {
			return nil, fmt.Errorf("processing-time TTL does not accept a field")
		}
		if options.Lifetime <= 0 {
			return nil, fmt.Errorf("processing-time TTL lifetime must be positive")
		}
		if options.Clock == nil {
			options.Clock = time.Now
		}
		return &typedTableTTLState{options: options, field: -1}, nil
	case TypedTableTTLEventTime:
		if options.Field == "" {
			return nil, fmt.Errorf("event-time TTL field is required")
		}
		if options.Lifetime <= 0 {
			return nil, fmt.Errorf("event-time TTL lifetime must be positive")
		}
		field, ok := byName[options.Field]
		if !ok {
			return nil, fmt.Errorf("event-time TTL field %q does not exist", options.Field)
		}
		if columns[field].Kind != TypedTableInt64 {
			return nil, fmt.Errorf("event-time TTL field %q must be an int64 Unix-nanosecond column", options.Field)
		}
		options.Field = columns[field].Name
		return &typedTableTTLState{options: options, field: field}, nil
	default:
		return nil, fmt.Errorf("invalid typed table TTL mode %d", options.Mode)
	}
}

func (state *typedTableTTLState) now() time.Time {
	if state == nil || state.options.Clock == nil {
		return time.Now()
	}
	return state.options.Clock()
}

func (state *typedTableTTLState) deadline(now time.Time) int64 {
	if state == nil || state.options.Mode != TypedTableTTLProcessingTime {
		return 0
	}
	return typedTableTTLDeadline(now, state.options.Lifetime)
}

func typedTableTTLDeadline(now time.Time, lifetime time.Duration) int64 {
	base := now.UnixNano()
	delta := int64(lifetime)
	if base > math.MaxInt64-delta {
		return math.MaxInt64
	}
	return base + delta
}

func newTypedTableColumnTTLState(index int, options TypedTableTTLOptions, columns []TypedTableColumn, byName map[string]int) (*typedTableColumnTTLState, error) {
	options.Field = strings.TrimSpace(options.Field)
	switch options.Mode {
	case TypedTableTTLDisabled:
		return nil, nil
	case TypedTableTTLProcessingTime:
		if options.Field != "" {
			return nil, fmt.Errorf("column %q processing-time TTL does not accept a field", columns[index].Name)
		}
		if options.Lifetime <= 0 {
			return nil, fmt.Errorf("column %q processing-time TTL lifetime must be positive", columns[index].Name)
		}
		if options.Clock == nil {
			options.Clock = time.Now
		}
		return &typedTableColumnTTLState{options: options, field: -1}, nil
	case TypedTableTTLEventTime:
		if options.Field == "" {
			return nil, fmt.Errorf("column %q event-time TTL field is required", columns[index].Name)
		}
		if options.Lifetime <= 0 {
			return nil, fmt.Errorf("column %q event-time TTL lifetime must be positive", columns[index].Name)
		}
		field, ok := byName[options.Field]
		if !ok {
			return nil, fmt.Errorf("column %q event-time TTL field %q does not exist", columns[index].Name, options.Field)
		}
		if columns[field].Kind != TypedTableInt64 {
			return nil, fmt.Errorf("column %q event-time TTL field %q must be an int64 Unix-nanosecond column", columns[index].Name, options.Field)
		}
		options.Field = columns[field].Name
		return &typedTableColumnTTLState{options: options, field: field}, nil
	default:
		return nil, fmt.Errorf("column %q has invalid TTL mode %d", columns[index].Name, options.Mode)
	}
}

func (state *typedTableColumnTTLState) now() time.Time {
	if state == nil || state.options.Clock == nil {
		return time.Now()
	}
	return state.options.Clock()
}

func (state *typedTableColumnTTLState) deadline(now time.Time) int64 {
	if state == nil || state.options.Mode != TypedTableTTLProcessingTime {
		return 0
	}
	return typedTableTTLDeadline(now, state.options.Lifetime)
}

func typedTableTTLExpired(value int64, lifetime time.Duration, now time.Time) bool {
	deadline, ok := typedTableTTLEventDeadline(value, int64(lifetime))
	if !ok {
		return false
	}
	return deadline <= now.UnixNano()
}

func (table *TypedTable) typedTableTTLNow() time.Time {
	if table == nil || table.ttl == nil {
		return time.Time{}
	}
	return table.ttl.now()
}

func (table *TypedTable) typedTableRowExpiredLocked(index int, now time.Time) bool {
	if table == nil || table.ttl == nil || index < 0 || index >= len(table.keys) {
		return false
	}
	switch table.ttl.options.Mode {
	case TypedTableTTLProcessingTime:
		return index < len(table.ttl.deadlines) && table.ttl.deadlines[index] <= now.UnixNano()
	case TypedTableTTLEventTime:
		value := table.columns[table.ttl.field].value(index)
		return value.Valid && typedTableTTLExpired(value.Int64, table.ttl.options.Lifetime, now)
	default:
		return false
	}
}

func (table *TypedTable) typedTableRowHiddenLocked(index int, now time.Time) bool {
	return table.typedTableRowDeletedLocked(index) || table.typedTableRowExpiredLocked(index, now)
}

func (table *TypedTable) setTypedTableTTLDeadlineLocked(index int, now time.Time) {
	if table == nil || table.ttl == nil {
		return
	}
	if table.ttl.options.Mode == TypedTableTTLProcessingTime {
		for len(table.ttl.deadlines) <= index {
			table.ttl.deadlines = append(table.ttl.deadlines, 0)
		}
		table.ttl.deadlines[index] = table.ttl.deadline(now)
	}
	deadline, ok := table.typedTableTTLDeadlineAtLocked(index)
	if !ok {
		table.ttl.expiryRemove(index)
		return
	}
	table.ttl.expiryUpsert(index, deadline)
}

func (table *TypedTable) setTypedTableColumnTTLDeadlineLocked(index int) {
	if table == nil || table.columnTTLs == nil {
		return
	}
	for _, state := range table.columnTTLs {
		if state == nil || state.options.Mode != TypedTableTTLProcessingTime {
			continue
		}
		for len(state.deadlines) <= index {
			state.deadlines = append(state.deadlines, 0)
		}
		state.deadlines[index] = state.deadline(state.now())
	}
}

func (table *TypedTable) moveTypedTableColumnTTLDeadlineLocked(index, from int) {
	if table == nil || table.columnTTLs == nil || index == from {
		return
	}
	for _, state := range table.columnTTLs {
		if state == nil || state.options.Mode != TypedTableTTLProcessingTime || from >= len(state.deadlines) || index >= len(state.deadlines) {
			continue
		}
		state.deadlines[index] = state.deadlines[from]
	}
}

func (table *TypedTable) truncateTypedTableColumnTTLDeadlinesLocked(length int) {
	if table == nil || table.columnTTLs == nil {
		return
	}
	for _, state := range table.columnTTLs {
		if state == nil || state.options.Mode != TypedTableTTLProcessingTime || length >= len(state.deadlines) {
			continue
		}
		state.deadlines = state.deadlines[:length]
	}
}

func (table *TypedTable) typedTableColumnExpiredAtLocked(column, row int, now time.Time) bool {
	if table == nil || table.columnTTLs == nil || column < 0 || column >= len(table.columnTTLs) || row < 0 || row >= len(table.keys) {
		return false
	}
	state := table.columnTTLs[column]
	if state == nil || !table.columns[column].valid[row] {
		return false
	}
	switch state.options.Mode {
	case TypedTableTTLProcessingTime:
		return row < len(state.deadlines) && state.deadlines[row] <= now.UnixNano()
	case TypedTableTTLEventTime:
		value := table.columns[state.field].value(row)
		return value.Valid && typedTableTTLExpired(value.Int64, state.options.Lifetime, now)
	default:
		return false
	}
}

func (table *TypedTable) typedTableColumnExpiredLocked(column, row int) bool {
	if table == nil || table.columnTTLs == nil || column < 0 || column >= len(table.columnTTLs) || table.columnTTLs[column] == nil {
		return false
	}
	return table.typedTableColumnExpiredAtLocked(column, row, table.columnTTLs[column].now())
}

func (table *TypedTable) maskTypedTableExpiredColumnsLocked(row int, values []TypedTableValue) {
	if table == nil || table.columnTTLs == nil {
		return
	}
	for column, state := range table.columnTTLs {
		if state != nil && table.typedTableColumnExpiredLocked(column, row) {
			values[column] = TypedNull()
		}
	}
}

// PurgeExpired removes rows whose configured TTL has elapsed at now. The
// returned changes are ordinary DELETE changefeed entries in physical row
// order, so incremental consumers can apply expiry exactly like a caller
// initiated delete. A nil or disabled TTL is a no-op.
func (table *TypedTable) PurgeExpired(now time.Time) ([]TypedTableChange, error) {
	if table == nil {
		return nil, fmt.Errorf("typed table is nil")
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	if table.ttl == nil {
		return nil, nil
	}
	expiredRows := make([]int, 0)
	nowUnixNano := now.UnixNano()
	for len(table.ttl.expiryHeap) > 0 && table.ttl.expiryHeap[0].deadline <= nowUnixNano {
		entry, ok := table.ttl.expiryPop()
		if !ok || entry.row < 0 || entry.row >= len(table.keys) || table.typedTableRowDeletedLocked(entry.row) {
			continue
		}
		if deadline, ok := table.typedTableTTLDeadlineAtLocked(entry.row); !ok || deadline > nowUnixNano {
			continue
		}
		expiredRows = append(expiredRows, entry.row)
	}
	if len(expiredRows) == 0 {
		return nil, nil
	}
	sort.Ints(expiredRows)
	expiredKeys := make([]string, len(expiredRows))
	for index, row := range expiredRows {
		expiredKeys[index] = table.keys[row]
	}
	changes := make([]TypedTableChange, 0, len(expiredKeys))
	prepared := false
	for _, key := range expiredKeys {
		index, exists := table.positions[key]
		if !exists || table.typedTableRowDeletedLocked(index) || !table.typedTableRowExpiredLocked(index, now) {
			continue
		}
		if !prepared {
			table.clearColumnarLayoutsLocked()
			table.invalidateTypedTableDerivedCachesLocked()
			table.appendOnly = false
			prepared = true
		}
		changes = append(changes, table.deleteIndexLocked(index))
	}
	if len(changes) == 0 {
		return nil, nil
	}
	return changes, nil
}

// PurgeExpiredColumns physically clears independently expired column values
// while retaining their rows. Each changed row emits one ordinary UPDATE so
// exact changefeed consumers can apply the same masking operation. A nil or
// column-TTL-disabled table is a no-op.
func (table *TypedTable) PurgeExpiredColumns(now time.Time) ([]TypedTableChange, error) {
	if table == nil {
		return nil, fmt.Errorf("typed table is nil")
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	if table.columnTTLs == nil {
		return nil, nil
	}
	type pendingColumnExpiry struct {
		row     int
		columns []int
	}
	pending := make([]pendingColumnExpiry, 0)
	for row := range table.keys {
		if table.typedTableRowHiddenLocked(row, now) {
			continue
		}
		expiredColumns := make([]int, 0)
		for column, state := range table.columnTTLs {
			if state != nil && table.typedTableColumnExpiredAtLocked(column, row, now) {
				expiredColumns = append(expiredColumns, column)
			}
		}
		if len(expiredColumns) > 0 {
			pending = append(pending, pendingColumnExpiry{row: row, columns: expiredColumns})
		}
	}
	if len(pending) == 0 {
		return nil, nil
	}
	table.clearColumnarLayoutsLocked()
	table.invalidateTypedTableDerivedCachesLocked()
	table.appendOnly = false
	changes := make([]TypedTableChange, 0, len(pending))
	for _, expiry := range pending {
		before := table.rowLocked(expiry.row)
		after := cloneTypedTableValues(before)
		for _, column := range expiry.columns {
			table.columns[column].set(expiry.row, TypedNull())
			after[column] = TypedNull()
		}
		changes = append(changes, table.appendChangeLocked(TypedTableChange{
			Operation: "UPDATE",
			Key:       table.keys[expiry.row],
			Before:    before,
			After:     after,
		}))
	}
	return changes, nil
}
