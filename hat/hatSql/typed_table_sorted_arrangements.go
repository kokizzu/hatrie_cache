package hatSql

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// TypedTableSortedArrangements shares ordered state among consumers whose
// ORDER BY definitions are compatible. An arrangement with a longer ordering
// can serve a shorter prefix because rows remain ordered by every requested
// field. Direction and NULL placement must match; dictionary options are
// storage hints and do not change the logical order.
type TypedTableSortedArrangements struct {
	mu      sync.Mutex
	table   *TypedTable
	entries map[string]*typedTableSortedArrangementEntry
}

type typedTableSortedArrangementEntry struct {
	mu          sync.Mutex
	arrangement *TypedTableSortedArrangement
	definition  TypedTableSortedArrangementDefinition
	references  int
}

// TypedTableSortedArrangementLease is one reference-counted lease on shared
// sorted state. Release it when the consumer no longer needs the arrangement.
type TypedTableSortedArrangementLease struct {
	mu       sync.Mutex
	owner    *TypedTableSortedArrangements
	key      string
	entry    *typedTableSortedArrangementEntry
	reused   bool
	released bool
}

// NewTypedTableSortedArrangements creates an empty sorted-arrangement
// registry for table.
func NewTypedTableSortedArrangements(table *TypedTable) (*TypedTableSortedArrangements, error) {
	if table == nil {
		return nil, fmt.Errorf("typed table sorted arrangements require a table")
	}
	return &TypedTableSortedArrangements{
		table:   table,
		entries: make(map[string]*typedTableSortedArrangementEntry),
	}, nil
}

// Acquire returns a lease for definition. An existing exact arrangement is
// preferred; otherwise the shortest compatible longer ordering is reused. If
// no compatible arrangement exists, one is built from the current table
// snapshot.
func (arrangements *TypedTableSortedArrangements) Acquire(definition TypedTableSortedArrangementDefinition) (*TypedTableSortedArrangementLease, error) {
	if arrangements == nil {
		return nil, fmt.Errorf("typed table sorted arrangements are nil")
	}
	if _, err := typedTableSortedArrangementOrderFields(arrangements.table, definition); err != nil {
		return nil, err
	}
	key := typedTableSortedArrangementDefinitionKey(definition)

	arrangements.mu.Lock()
	defer arrangements.mu.Unlock()
	entry, entryKey, reused := arrangements.findCompatibleLocked(definition, key)
	if entry == nil {
		arrangement, err := NewTypedTableSortedArrangement(arrangements.table, definition)
		if err != nil {
			return nil, err
		}
		entry = &typedTableSortedArrangementEntry{
			arrangement: arrangement,
			definition:  cloneTypedTableSortedArrangementDefinition(definition),
		}
		arrangements.entries[key] = entry
		entryKey = key
	}
	entry.references++
	return &TypedTableSortedArrangementLease{
		owner:  arrangements,
		key:    entryKey,
		entry:  entry,
		reused: reused,
	}, nil
}

// Active returns the number of distinct sorted arrangements held by at least
// one lease.
func (arrangements *TypedTableSortedArrangements) Active() int {
	if arrangements == nil {
		return 0
	}
	arrangements.mu.Lock()
	defer arrangements.mu.Unlock()
	return len(arrangements.entries)
}

func (arrangements *TypedTableSortedArrangements) findCompatibleLocked(
	requested TypedTableSortedArrangementDefinition,
	requestedKey string,
) (*typedTableSortedArrangementEntry, string, bool) {
	if entry := arrangements.entries[requestedKey]; entry != nil {
		return entry, requestedKey, true
	}
	var selected *typedTableSortedArrangementEntry
	selectedKey := ""
	selectedLength := 0
	for key, entry := range arrangements.entries {
		if !typedTableSortedArrangementDefinitionsCompatible(entry.definition, requested) {
			continue
		}
		length := len(typedTableSortedArrangementDefinitionOrder(entry.definition))
		if selected == nil || length < selectedLength || length == selectedLength && key < selectedKey {
			selected = entry
			selectedKey = key
			selectedLength = length
		}
	}
	if selected == nil {
		return nil, "", false
	}
	return selected, selectedKey, true
}

// Apply advances the shared sorted arrangement through source changes.
func (lease *TypedTableSortedArrangementLease) Apply(changes []TypedTableChange) error {
	entry, err := lease.activeEntry()
	if err != nil {
		return err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return entry.arrangement.Apply(changes)
}

// Checkpoint returns the shared arrangement's last applied source sequence.
func (lease *TypedTableSortedArrangementLease) Checkpoint() uint64 {
	entry, err := lease.activeEntry()
	if err != nil {
		return 0
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return entry.arrangement.Checkpoint()
}

// Rows returns an independent snapshot in the shared arrangement's full
// ordering. A shorter requested prefix is still valid because the full order
// is a refinement of that prefix.
func (lease *TypedTableSortedArrangementLease) Rows() []TypedTableMergeJoinInput {
	entry, err := lease.activeEntry()
	if err != nil {
		return nil
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return entry.arrangement.Rows()
}

// RowsPage returns an independent bounded snapshot in the shared ordering.
func (lease *TypedTableSortedArrangementLease) RowsPage(offset, limit int) []TypedTableMergeJoinInput {
	entry, err := lease.activeEntry()
	if err != nil {
		return nil
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return entry.arrangement.RowsPage(offset, limit)
}

// Definition returns the arrangement definition owned by this lease. The
// returned composite order is copied and can be safely modified by callers.
func (lease *TypedTableSortedArrangementLease) Definition() TypedTableSortedArrangementDefinition {
	entry, err := lease.activeEntry()
	if err != nil {
		return TypedTableSortedArrangementDefinition{}
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return cloneTypedTableSortedArrangementDefinition(entry.definition)
}

// Reused reports whether Acquire found an existing compatible arrangement.
func (lease *TypedTableSortedArrangementLease) Reused() bool {
	if lease == nil {
		return false
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	return lease.reused
}

// Release drops this lease. It returns false for nil or already released
// leases. Shared state is discarded after the final lease is released.
func (lease *TypedTableSortedArrangementLease) Release() bool {
	if lease == nil {
		return false
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.released || lease.owner == nil || lease.entry == nil {
		return false
	}
	owner := lease.owner
	owner.mu.Lock()
	defer owner.mu.Unlock()
	entry := lease.entry
	if current := owner.entries[lease.key]; current != entry || entry.references <= 0 {
		return false
	}
	entry.references--
	if entry.references == 0 {
		delete(owner.entries, lease.key)
	}
	lease.released = true
	return true
}

func (lease *TypedTableSortedArrangementLease) activeEntry() (*typedTableSortedArrangementEntry, error) {
	if lease == nil {
		return nil, fmt.Errorf("typed table sorted arrangement lease is nil")
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.released || lease.entry == nil {
		return nil, fmt.Errorf("typed table sorted arrangement lease is released")
	}
	return lease.entry, nil
}

func typedTableSortedArrangementDefinitionOrder(definition TypedTableSortedArrangementDefinition) []TypedTableSortedArrangementOrder {
	if len(definition.OrderBy) != 0 {
		return definition.OrderBy
	}
	return []TypedTableSortedArrangementOrder{{
		Field:              definition.Field,
		Descending:         definition.Descending,
		NullsFirst:         definition.NullsFirst,
		DictionaryEncoded:  definition.DictionaryEncoded,
		DictionaryAdaptive: definition.DictionaryAdaptive,
	}}
}

func typedTableSortedArrangementDefinitionsCompatible(existing, requested TypedTableSortedArrangementDefinition) bool {
	existingOrder := typedTableSortedArrangementDefinitionOrder(existing)
	requestedOrder := typedTableSortedArrangementDefinitionOrder(requested)
	if len(existingOrder) < len(requestedOrder) {
		return false
	}
	for index, requestedField := range requestedOrder {
		existingField := existingOrder[index]
		if existingField.Field != requestedField.Field ||
			existingField.Descending != requestedField.Descending ||
			existingField.NullsFirst != requestedField.NullsFirst {
			return false
		}
	}
	return true
}

func typedTableSortedArrangementDefinitionKey(definition TypedTableSortedArrangementDefinition) string {
	var builder strings.Builder
	for _, order := range typedTableSortedArrangementDefinitionOrder(definition) {
		builder.WriteString(strconv.Itoa(len(order.Field)))
		builder.WriteByte(':')
		builder.WriteString(order.Field)
		if order.Descending {
			builder.WriteByte('d')
		} else {
			builder.WriteByte('a')
		}
		if order.NullsFirst {
			builder.WriteByte('n')
		} else {
			builder.WriteByte('l')
		}
		builder.WriteByte(';')
	}
	return builder.String()
}

func cloneTypedTableSortedArrangementDefinition(definition TypedTableSortedArrangementDefinition) TypedTableSortedArrangementDefinition {
	definition.OrderBy = append([]TypedTableSortedArrangementOrder(nil), definition.OrderBy...)
	return definition
}
