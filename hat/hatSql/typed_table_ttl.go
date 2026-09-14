package hatSql

import (
	"fmt"
	"math"
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
	base := now.UnixNano()
	delta := int64(state.options.Lifetime)
	if base > math.MaxInt64-delta {
		return math.MaxInt64
	}
	return base + delta
}

func typedTableTTLExpired(value int64, lifetime time.Duration, now time.Time) bool {
	delta := int64(lifetime)
	if value > math.MaxInt64-delta {
		return false
	}
	return value+delta <= now.UnixNano()
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
	if table == nil || table.ttl == nil || table.ttl.options.Mode != TypedTableTTLProcessingTime {
		return
	}
	for len(table.ttl.deadlines) <= index {
		table.ttl.deadlines = append(table.ttl.deadlines, 0)
	}
	table.ttl.deadlines[index] = table.ttl.deadline(now)
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
	changes := make([]TypedTableChange, 0)
	prepared := false
	for index := 0; index < len(table.keys); {
		if table.typedTableRowDeletedLocked(index) || !table.typedTableRowExpiredLocked(index, now) {
			index++
			continue
		}
		if !prepared {
			table.clearColumnarLayoutsLocked()
			table.invalidateTypedTableDerivedCachesLocked()
			table.appendOnly = false
			prepared = true
		}
		changes = append(changes, table.deleteIndexLocked(index))
		if table.patchParts != nil {
			index++
		}
	}
	if len(changes) == 0 {
		return nil, nil
	}
	return changes, nil
}
