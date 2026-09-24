package hatSql

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const DefaultTypedTableArrangementHydrationBatch = 1024

// TypedTableAggregateArrangementFreshness reports an arrangement checkpoint
// against the current typed-table changefeed tail.
type TypedTableAggregateArrangementFreshness struct {
	Checkpoint     uint64 `json:"checkpoint"`
	SourceSequence uint64 `json:"source_sequence"`
	Stale          bool   `json:"stale"`
}

// TypedTableAggregateArrangementHydration reports one bounded changefeed
// replay. Complete is false when another Hydrate call is required.
type TypedTableAggregateArrangementHydration struct {
	Before         uint64 `json:"before"`
	After          uint64 `json:"after"`
	SourceSequence uint64 `json:"source_sequence"`
	Applied        int    `json:"applied"`
	Complete       bool   `json:"complete"`
}

// TypedTableAggregateArrangements shares exact TypedTableAggregate state among
// consumers that use the same ordered GROUP BY, SUM, MIN, MAX, and COUNT
// DISTINCT definition. It does not apply table changes automatically; callers
// retain control of checkpoint scheduling and changefeed compaction.
type TypedTableAggregateArrangements struct {
	mu      sync.Mutex
	table   *TypedTable
	entries map[uint64][]*typedTableAggregateArrangementEntry
}

type typedTableAggregateArrangementEntry struct {
	mu         sync.Mutex
	aggregate  *TypedTableAggregate
	references int
	hydration  typedTableArrangementHydrationState
	definition typedTableAggregateArrangementDefinition
}

type typedTableAggregateArrangementDefinition struct {
	groupBy                []string
	sumField               string
	minField               string
	maxField               string
	distinctField          string
	sumConfigured          bool
	minConfigured          bool
	maxConfigured          bool
	distinctConfigured     bool
	dictionaryEncodeGroups bool
}

// TypedTableAggregateArrangement is one reference-counted lease on shared
// exact aggregate state. Release it when the consumer no longer needs it.
type TypedTableAggregateArrangement struct {
	mu       sync.Mutex
	owner    *TypedTableAggregateArrangements
	hash     uint64
	entry    *typedTableAggregateArrangementEntry
	released bool
}

// NewTypedTableAggregateArrangements creates an empty registry for table.
func NewTypedTableAggregateArrangements(table *TypedTable) (*TypedTableAggregateArrangements, error) {
	if table == nil {
		return nil, fmt.Errorf("typed table aggregate arrangements require a table")
	}
	return &TypedTableAggregateArrangements{table: table, entries: make(map[uint64][]*typedTableAggregateArrangementEntry)}, nil
}

// Acquire returns a lease for the exact aggregate definition. Equivalent
// definitions share one aggregate and therefore one ordered checkpoint.
func (arrangements *TypedTableAggregateArrangements) Acquire(definition TypedTableAggregateDefinition) (*TypedTableAggregateArrangement, error) {
	if arrangements == nil {
		return nil, fmt.Errorf("typed table aggregate arrangements are nil")
	}
	canonical := typedTableAggregateArrangementDefinitionOf(definition)
	hash := typedTableAggregateArrangementHash(canonical)
	arrangements.mu.Lock()
	defer arrangements.mu.Unlock()
	if entry := typedTableAggregateArrangementEntryForDefinitionLocked(arrangements, canonical, hash); entry != nil {
		entry.references++
		return &TypedTableAggregateArrangement{owner: arrangements, hash: hash, entry: entry}, nil
	}
	aggregate, err := NewTypedTableAggregate(arrangements.table, definition)
	if err != nil {
		return nil, err
	}
	entry := &typedTableAggregateArrangementEntry{
		aggregate:  aggregate,
		hydration:  newTypedTableArrangementHydrationState(),
		definition: canonical,
	}
	arrangements.entries[hash] = append(arrangements.entries[hash], entry)
	entry.references++
	return &TypedTableAggregateArrangement{owner: arrangements, hash: hash, entry: entry}, nil
}

func typedTableAggregateArrangementEntryForDefinitionLocked(arrangements *TypedTableAggregateArrangements, definition typedTableAggregateArrangementDefinition, hash uint64) *typedTableAggregateArrangementEntry {
	for _, entry := range arrangements.entries[hash] {
		if typedTableAggregateArrangementDefinitionEqual(entry.definition, definition) {
			return entry
		}
	}
	return nil
}

func sortedTypedTableAggregateArrangementEntriesLocked(arrangements *TypedTableAggregateArrangements) []*typedTableAggregateArrangementEntry {
	entries := make([]*typedTableAggregateArrangementEntry, 0, len(arrangements.entries))
	for _, bucket := range arrangements.entries {
		entries = append(entries, bucket...)
	}
	sort.Slice(entries, func(left, right int) bool {
		return typedTableAggregateArrangementDefinitionKey(entries[left].definition) < typedTableAggregateArrangementDefinitionKey(entries[right].definition)
	})
	return entries
}

// Active returns the number of distinct aggregate definitions currently held
// by at least one lease.
func (arrangements *TypedTableAggregateArrangements) Active() int {
	if arrangements == nil {
		return 0
	}
	arrangements.mu.Lock()
	defer arrangements.mu.Unlock()
	active := 0
	for _, bucket := range arrangements.entries {
		active += len(bucket)
	}
	return active
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

// Freshness compares this arrangement with the source table's current change
// sequence without changing arrangement state.
func (arrangement *TypedTableAggregateArrangement) Freshness() (TypedTableAggregateArrangementFreshness, error) {
	entry, err := arrangement.activeEntry()
	if err != nil {
		return TypedTableAggregateArrangementFreshness{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	entry.aggregate.table.mu.RLock()
	sourceSequence := entry.aggregate.table.sequence
	entry.aggregate.table.mu.RUnlock()
	checkpoint := entry.aggregate.checkpoint
	return TypedTableAggregateArrangementFreshness{
		Checkpoint:     checkpoint,
		SourceSequence: sourceSequence,
		Stale:          checkpoint < sourceSequence,
	}, nil
}

// Hydrate replays up to limit retained source changes into this arrangement.
// A zero limit uses DefaultTypedTableArrangementHydrationBatch. If the source
// changelog has already been compacted past the arrangement checkpoint, the
// existing ErrTypedTableChangesCompacted error is returned and no partial
// rebuild is attempted.
func (arrangement *TypedTableAggregateArrangement) Hydrate(limit int) (TypedTableAggregateArrangementHydration, error) {
	if limit < 0 {
		return TypedTableAggregateArrangementHydration{}, fmt.Errorf("typed table arrangement hydration batch cannot be negative")
	}
	if limit == 0 {
		limit = DefaultTypedTableArrangementHydrationBatch
	}
	entry, err := arrangement.activeEntry()
	if err != nil {
		return TypedTableAggregateArrangementHydration{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	entry.hydration.resetLocked()
	before := entry.aggregate.checkpoint
	changes, sourceSequence, err := entry.aggregate.table.ChangesAfter(before, limit)
	if err != nil {
		entry.hydration.failLocked(err)
		return TypedTableAggregateArrangementHydration{}, err
	}
	if err := entry.aggregate.Apply(changes); err != nil {
		entry.hydration.failLocked(err)
		return TypedTableAggregateArrangementHydration{}, err
	}
	after := entry.aggregate.checkpoint
	entry.hydration.completeLocked()
	return TypedTableAggregateArrangementHydration{
		Before:         before,
		After:          after,
		SourceSequence: sourceSequence,
		Applied:        len(changes),
		Complete:       after >= sourceSequence,
	}, nil
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
	bucket := owner.entries[arrangement.hash]
	entryIndex := -1
	for index, current := range bucket {
		if current == entry {
			entryIndex = index
			break
		}
	}
	if entryIndex < 0 || entry.references <= 0 {
		return false
	}
	entry.references--
	if entry.references == 0 {
		copy(bucket[entryIndex:], bucket[entryIndex+1:])
		bucket[len(bucket)-1] = nil
		bucket = bucket[:len(bucket)-1]
		if len(bucket) == 0 {
			delete(owner.entries, arrangement.hash)
		} else {
			owner.entries[arrangement.hash] = bucket
		}
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

func typedTableAggregateArrangementDefinitionOf(definition TypedTableAggregateDefinition) typedTableAggregateArrangementDefinition {
	canonical := typedTableAggregateArrangementDefinition{
		groupBy:                make([]string, len(definition.GroupBy)),
		sumField:               strings.TrimSpace(definition.SumField),
		minField:               strings.TrimSpace(definition.MinField),
		maxField:               strings.TrimSpace(definition.MaxField),
		distinctField:          strings.TrimSpace(definition.DistinctField),
		sumConfigured:          definition.SumField != "",
		minConfigured:          definition.MinField != "",
		maxConfigured:          definition.MaxField != "",
		distinctConfigured:     definition.DistinctField != "",
		dictionaryEncodeGroups: definition.DictionaryEncodeGroups,
	}
	for index, field := range definition.GroupBy {
		canonical.groupBy[index] = strings.TrimSpace(field)
	}
	return canonical
}

func typedTableAggregateArrangementDefinitionEqual(left, right typedTableAggregateArrangementDefinition) bool {
	if left.sumField != right.sumField || left.minField != right.minField || left.maxField != right.maxField || left.distinctField != right.distinctField || left.sumConfigured != right.sumConfigured || left.minConfigured != right.minConfigured || left.maxConfigured != right.maxConfigured || left.distinctConfigured != right.distinctConfigured || left.dictionaryEncodeGroups != right.dictionaryEncodeGroups || len(left.groupBy) != len(right.groupBy) {
		return false
	}
	for index := range left.groupBy {
		if left.groupBy[index] != right.groupBy[index] {
			return false
		}
	}
	return true
}

func typedTableAggregateArrangementHash(definition typedTableAggregateArrangementDefinition) uint64 {
	const (
		fnvOffset64 = uint64(14695981039346656037)
		fnvPrime64  = uint64(1099511628211)
	)
	hash := fnvOffset64
	addByte := func(value byte) {
		hash ^= uint64(value)
		hash *= fnvPrime64
	}
	addString := func(value string) {
		for index := 0; index < len(value); index++ {
			addByte(value[index])
		}
		addByte(0)
	}
	for _, field := range definition.groupBy {
		addString(field)
		addByte('|')
	}
	addByte(';')
	addField := func(configured bool, field string) {
		if configured {
			addByte(1)
		} else {
			addByte(0)
		}
		addString(field)
		addByte(';')
	}
	addField(definition.sumConfigured, definition.sumField)
	addField(definition.minConfigured, definition.minField)
	addField(definition.maxConfigured, definition.maxField)
	addField(definition.distinctConfigured, definition.distinctField)
	if definition.dictionaryEncodeGroups {
		addByte(1)
	} else {
		addByte(0)
	}
	return hash
}

func typedTableAggregateArrangementKey(definition TypedTableAggregateDefinition) string {
	return typedTableAggregateArrangementDefinitionKey(typedTableAggregateArrangementDefinitionOf(definition))
}

func typedTableAggregateArrangementDefinitionKey(definition typedTableAggregateArrangementDefinition) string {
	var builder strings.Builder
	for _, field := range definition.groupBy {
		builder.WriteString(strconv.Itoa(len(field)))
		builder.WriteByte(':')
		builder.WriteString(field)
		builder.WriteByte('|')
	}
	builder.WriteByte(';')
	writeField := func(configured bool, field string) {
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
	writeField(definition.sumConfigured, definition.sumField)
	writeField(definition.minConfigured, definition.minField)
	writeField(definition.maxConfigured, definition.maxField)
	writeField(definition.distinctConfigured, definition.distinctField)
	if definition.dictionaryEncodeGroups {
		builder.WriteByte('1')
	} else {
		builder.WriteByte('0')
	}
	return builder.String()
}
