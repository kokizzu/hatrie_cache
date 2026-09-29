package hatSql

import "sort"

// TypedTableAggregateArrangementStats describes one shared aggregate state
// without materializing its result rows. EstimatedBytes is a bounded
// accounting estimate for retained group metadata and values, not a runtime
// allocator reading.
type TypedTableAggregateArrangementStats struct {
	DefinitionKey    string `json:"definition_key"`
	References       int    `json:"references"`
	Checkpoint       uint64 `json:"checkpoint"`
	SourceSequence   uint64 `json:"source_sequence"`
	CompactedThrough uint64 `json:"compacted_through"`
	Groups           int    `json:"groups"`
	DistinctValues   int    `json:"distinct_values"`
	CompactionCount  uint64 `json:"compaction_count"`
	KeyBytes         uint64 `json:"key_bytes"`
	ValueBytes       uint64 `json:"value_bytes"`
	TraceBytes       uint64 `json:"trace_bytes"`
	EstimatedBytes   uint64 `json:"estimated_bytes"`
	RetainedBytes    uint64 `json:"retained_bytes"`
}

// TypedTableAggregateArrangementsStats is a consistent point-in-time report
// of all active shared aggregate arrangements for one typed table.
type TypedTableAggregateArrangementsStats struct {
	SourceSequence    uint64                                `json:"source_sequence"`
	ActiveDefinitions int                                   `json:"active_definitions"`
	ActiveLeases      int                                   `json:"active_leases"`
	Arrangements      []TypedTableAggregateArrangementStats `json:"arrangements"`
}

// Stats returns arrangement references, checkpoints, group counts, and
// estimated retained bytes without changing any aggregate state. The returned
// slice is independent of the registry and sorted by definition key.
func (arrangements *TypedTableAggregateArrangements) Stats() TypedTableAggregateArrangementsStats {
	if arrangements == nil {
		return TypedTableAggregateArrangementsStats{}
	}
	arrangements.mu.Lock()
	defer arrangements.mu.Unlock()
	entries := sortedTypedTableAggregateArrangementEntriesLocked(arrangements)
	stats := TypedTableAggregateArrangementsStats{
		ActiveDefinitions: len(entries),
		Arrangements:      make([]TypedTableAggregateArrangementStats, 0, len(entries)),
	}
	var compactedThrough uint64
	stats.SourceSequence, compactedThrough = typedTableAggregateTableState(arrangements.table)
	for _, entry := range entries {
		entry.mu.Lock()
		arrangementStats := typedTableAggregateArrangementStats(typedTableAggregateArrangementDefinitionKey(entry.definition), entry.references, entry.aggregate, stats.SourceSequence, compactedThrough)
		entry.mu.Unlock()
		stats.ActiveLeases += entry.references
		stats.Arrangements = append(stats.Arrangements, arrangementStats)
	}
	sort.Slice(stats.Arrangements, func(left, right int) bool {
		return stats.Arrangements[left].DefinitionKey < stats.Arrangements[right].DefinitionKey
	})
	return stats
}

// Stats returns this lease's current shared-state report. A released lease
// returns an error rather than exposing discarded state.
func (arrangement *TypedTableAggregateArrangement) Stats() (TypedTableAggregateArrangementStats, error) {
	entry, err := arrangement.activeEntry()
	if err != nil {
		return TypedTableAggregateArrangementStats{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	var table *TypedTable
	if entry.aggregate != nil {
		table = entry.aggregate.table
	}
	sourceSequence, compactedThrough := typedTableAggregateTableState(table)
	return typedTableAggregateArrangementStats(typedTableAggregateArrangementDefinitionKey(entry.definition), entry.references, entry.aggregate, sourceSequence, compactedThrough), nil
}

func typedTableAggregateArrangementStats(key string, references int, aggregate *TypedTableAggregate, sourceSequence, compactedThrough uint64) TypedTableAggregateArrangementStats {
	if aggregate == nil {
		return TypedTableAggregateArrangementStats{DefinitionKey: key, References: references}
	}
	distinctValues := 0
	for _, bucket := range aggregate.groups {
		distinctValues += len(bucket.group.distinctValues)
		for _, collision := range bucket.collisions {
			distinctValues += len(collision.distinctValues)
		}
	}
	keyBytes, valueBytes := estimateTypedTableAggregateStateBytes(aggregate)
	estimatedBytes := keyBytes
	addEstimatedBytes(&estimatedBytes, valueBytes)
	traceBytes := estimateTypedTableAggregateTraceBytes(aggregate)
	retainedBytes := estimatedBytes
	addEstimatedBytes(&retainedBytes, traceBytes)
	return TypedTableAggregateArrangementStats{
		DefinitionKey:    key,
		References:       references,
		Checkpoint:       aggregate.checkpoint,
		SourceSequence:   sourceSequence,
		CompactedThrough: compactedThrough,
		Groups:           aggregate.groupCount,
		DistinctValues:   distinctValues,
		CompactionCount:  aggregate.compactionCount,
		KeyBytes:         keyBytes,
		ValueBytes:       valueBytes,
		TraceBytes:       traceBytes,
		EstimatedBytes:   estimatedBytes,
		RetainedBytes:    retainedBytes,
	}
}

// TypedTableCompactionStats reports changefeed retention against the current
// logical frontier without adding fields or allocations to the hot arrangement
// stats path.
type TypedTableCompactionStats struct {
	LogicalFrontier  uint64 `json:"logical_frontier"`
	CompactedThrough uint64 `json:"compacted_through"`
	CompactionDebt   uint64 `json:"compaction_debt"`
}

// CompactionStats reports the table-level retained sequence distance for all
// aggregate arrangements in this catalog.
func (arrangements *TypedTableAggregateArrangements) CompactionStats() TypedTableCompactionStats {
	if arrangements == nil {
		return TypedTableCompactionStats{}
	}
	logicalFrontier, compactedThrough := typedTableAggregateTableState(arrangements.table)
	return newTypedTableCompactionStats(logicalFrontier, compactedThrough)
}

// CompactionStats reports the table-level retained sequence distance for this
// aggregate arrangement lease.
func (arrangement *TypedTableAggregateArrangement) CompactionStats() (TypedTableCompactionStats, error) {
	entry, err := arrangement.activeEntry()
	if err != nil {
		return TypedTableCompactionStats{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	var table *TypedTable
	if entry.aggregate != nil {
		table = entry.aggregate.table
	}
	logicalFrontier, compactedThrough := typedTableAggregateTableState(table)
	return newTypedTableCompactionStats(logicalFrontier, compactedThrough), nil
}

func newTypedTableCompactionStats(logicalFrontier, compactedThrough uint64) TypedTableCompactionStats {
	return TypedTableCompactionStats{
		LogicalFrontier:  logicalFrontier,
		CompactedThrough: compactedThrough,
		CompactionDebt:   typedTableCompactionDebt(logicalFrontier, compactedThrough),
	}
}

// typedTableCompactionDebt reports retained changefeed sequence distance from
// the current logical frontier to the table's compaction frontier. It is a
// sequence count, not a byte estimate, and saturates at zero if frontiers are
// already equal or a restored snapshot reports them out of order.
func typedTableCompactionDebt(sourceSequence, compactedThrough uint64) uint64 {
	if sourceSequence <= compactedThrough {
		return 0
	}
	return sourceSequence - compactedThrough
}

func typedTableAggregateTableState(table *TypedTable) (sourceSequence, compactedThrough uint64) {
	if table == nil {
		return 0, 0
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	return table.sequence, table.compactedThrough
}

func estimateTypedTableAggregateStateBytes(aggregate *TypedTableAggregate) (keyBytes, valueBytes uint64) {
	if aggregate == nil {
		return 0, 0
	}
	addEstimatedCount(&keyBytes, len(aggregate.groups), 80)
	addEstimatedCount(&keyBytes, len(aggregate.compactGroupOrder), 16)
	addEstimatedCount(&keyBytes, len(aggregate.groupDictionaries), 64)
	for _, bucket := range aggregate.groups {
		groupKeyBytes, groupValueBytes := estimateTypedTableAggregateGroupBytes(bucket.group)
		addEstimatedBytes(&keyBytes, groupKeyBytes)
		addEstimatedBytes(&valueBytes, groupValueBytes)
		for _, collision := range bucket.collisions {
			groupKeyBytes, groupValueBytes := estimateTypedTableAggregateGroupBytes(collision)
			addEstimatedBytes(&keyBytes, groupKeyBytes)
			addEstimatedBytes(&valueBytes, groupValueBytes)
		}
	}
	return keyBytes, valueBytes
}

func estimateTypedTableAggregateGroupBytes(group typedTableAggregateGroup) (keyBytes, valueBytes uint64) {
	keyBytes = 64
	addEstimatedBytes(&keyBytes, uint64(len(group.key)))
	addEstimatedCount(&keyBytes, len(group.values), 32)
	addEstimatedCount(&keyBytes, len(group.codes), 4)
	addEstimatedCount(&keyBytes, len(group.kinds), 1)
	valueBytes = 32
	addEstimatedCount(&valueBytes, len(group.minValues)+len(group.maxValues)+len(group.distinctValues), 64)
	return keyBytes, valueBytes
}

// estimateTypedTableAggregateTraceBytes reports the retained source
// changefeed footprint shared by this arrangement's table. It intentionally
// uses a fixed per-entry base so stats remain O(1) in retained history length;
// this is a bounded accounting estimate, not a runtime allocator reading.
func estimateTypedTableAggregateTraceBytes(aggregate *TypedTableAggregate) uint64 {
	if aggregate == nil || aggregate.table == nil {
		return 0
	}
	table := aggregate.table
	table.mu.RLock()
	defer table.mu.RUnlock()
	var total uint64
	addEstimatedCount(&total, len(table.changes), 64)
	return total
}

func addEstimatedCount(total *uint64, count int, bytesPerItem uint64) {
	if count <= 0 || bytesPerItem == 0 || *total == ^uint64(0) {
		return
	}
	countUint := uint64(count)
	if countUint > ^uint64(0)/bytesPerItem {
		*total = ^uint64(0)
		return
	}
	addEstimatedBytes(total, countUint*bytesPerItem)
}

func addEstimatedBytes(total *uint64, value uint64) {
	if ^uint64(0)-*total < value {
		*total = ^uint64(0)
		return
	}
	*total += value
}
