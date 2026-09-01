package hatSql

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// TypedTableAggregateArrangements shares exact TypedTableAggregate state among
// consumers that use the same ordered GROUP BY, SUM, MIN, and MAX definition.
// Min and max definitions retain counted numeric values. It does not apply
// table changes automatically; callers retain control of checkpoint scheduling
// and changefeed compaction.
type TypedTableAggregateArrangements struct {
	mu      sync.Mutex
	table   *TypedTable
	entries map[string]*typedTableAggregateArrangementEntry
}

type typedTableAggregateArrangementEntry struct {
	mu         sync.Mutex
	aggregate  *TypedTableAggregate
	references int
}

// TypedTableAggregateArrangement is one reference-counted lease on shared
// exact aggregate state. Release it when the consumer no longer needs it.
type TypedTableAggregateArrangement struct {
	mu       sync.Mutex
	owner    *TypedTableAggregateArrangements
	key      string
	entry    *typedTableAggregateArrangementEntry
	released bool
}

// NewTypedTableAggregateArrangements creates an empty registry for table.
func NewTypedTableAggregateArrangements(table *TypedTable) (*TypedTableAggregateArrangements, error) {
	if table == nil {
		return nil, fmt.Errorf("typed table aggregate arrangements require a table")
	}
	return &TypedTableAggregateArrangements{table: table, entries: make(map[string]*typedTableAggregateArrangementEntry)}, nil
}

// Acquire returns a lease for the exact aggregate definition. Equivalent
// definitions share one aggregate and therefore one ordered checkpoint.
func (arrangements *TypedTableAggregateArrangements) Acquire(definition TypedTableAggregateDefinition) (*TypedTableAggregateArrangement, error) {
	if arrangements == nil {
		return nil, fmt.Errorf("typed table aggregate arrangements are nil")
	}
	key := typedTableAggregateArrangementKey(definition)
	arrangements.mu.Lock()
	defer arrangements.mu.Unlock()
	entry := arrangements.entries[key]
	if entry == nil {
		aggregate, err := NewTypedTableAggregate(arrangements.table, definition)
		if err != nil {
			return nil, err
		}
		entry = &typedTableAggregateArrangementEntry{aggregate: aggregate}
		arrangements.entries[key] = entry
	}
	entry.references++
	return &TypedTableAggregateArrangement{owner: arrangements, key: key, entry: entry}, nil
}

// Active returns the number of distinct aggregate definitions currently held
// by at least one lease.
func (arrangements *TypedTableAggregateArrangements) Active() int {
	if arrangements == nil {
		return 0
	}
	arrangements.mu.Lock()
	defer arrangements.mu.Unlock()
	return len(arrangements.entries)
}

// Apply advances the shared aggregate through ordered table changes. Its
// replay and gap behavior is identical to TypedTableAggregate.Apply.
func (arrangement *TypedTableAggregateArrangement) Apply(changes []TypedTableChange) error {
	entry, err := arrangement.activeEntry()
	if err != nil {
		return err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return entry.aggregate.Apply(changes)
}

// Checkpoint returns the shared aggregate's last fully applied sequence.
func (arrangement *TypedTableAggregateArrangement) Checkpoint() uint64 {
	entry, err := arrangement.activeEntry()
	if err != nil {
		return 0
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return entry.aggregate.Checkpoint()
}

// Rows returns a deterministic independent snapshot of the shared aggregate.
func (arrangement *TypedTableAggregateArrangement) Rows() []Row {
	entry, err := arrangement.activeEntry()
	if err != nil {
		return nil
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return entry.aggregate.Rows()
}

// Release drops this lease. It returns false for nil or already released
// leases. The shared aggregate is discarded once the final lease is released.
func (arrangement *TypedTableAggregateArrangement) Release() bool {
	if arrangement == nil {
		return false
	}
	arrangement.mu.Lock()
	defer arrangement.mu.Unlock()
	if arrangement.released || arrangement.owner == nil || arrangement.entry == nil {
		return false
	}
	owner := arrangement.owner
	owner.mu.Lock()
	defer owner.mu.Unlock()
	entry := arrangement.entry
	if current := owner.entries[arrangement.key]; current != entry || entry.references <= 0 {
		return false
	}
	entry.references--
	if entry.references == 0 {
		delete(owner.entries, arrangement.key)
	}
	arrangement.released = true
	return true
}

func (arrangement *TypedTableAggregateArrangement) activeEntry() (*typedTableAggregateArrangementEntry, error) {
	if arrangement == nil {
		return nil, fmt.Errorf("typed table aggregate arrangement is nil")
	}
	arrangement.mu.Lock()
	defer arrangement.mu.Unlock()
	if arrangement.released || arrangement.entry == nil {
		return nil, fmt.Errorf("typed table aggregate arrangement is released")
	}
	return arrangement.entry, nil
}

func typedTableAggregateArrangementKey(definition TypedTableAggregateDefinition) string {
	var builder strings.Builder
	for _, field := range definition.GroupBy {
		field = strings.TrimSpace(field)
		builder.WriteString(strconv.Itoa(len(field)))
		builder.WriteByte(':')
		builder.WriteString(field)
		builder.WriteByte('|')
	}
	builder.WriteByte(';')
	for _, field := range []string{definition.SumField, definition.MinField, definition.MaxField} {
		configured := field != ""
		field = strings.TrimSpace(field)
		if configured {
			builder.WriteByte('1')
		} else {
			builder.WriteByte('0')
		}
		builder.WriteString(strconv.Itoa(len(field)))
		builder.WriteByte(':')
		builder.WriteString(field)
		builder.WriteByte(';')
	}
	return builder.String()
}
