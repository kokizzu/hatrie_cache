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
	EstimatedBytes   uint64 `json:"estimated_bytes"`
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
	stats := TypedTableAggregateArrangementsStats{
		ActiveDefinitions: len(arrangements.entries),
		Arrangements:      make([]TypedTableAggregateArrangementStats, 0, len(arrangements.entries)),
	}
	var compactedThrough uint64
	stats.SourceSequence, compactedThrough = typedTableAggregateTableState(arrangements.table)
	for key, entry := range arrangements.entries {
		entry.mu.Lock()
		arrangementStats := typedTableAggregateArrangementStats(key, entry.references, entry.aggregate, stats.SourceSequence, compactedThrough)
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
	return typedTableAggregateArrangementStats(arrangement.key, entry.references, entry.aggregate, sourceSequence, compactedThrough), nil
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
	return TypedTableAggregateArrangementStats{
		DefinitionKey:    key,
		References:       references,
		Checkpoint:       aggregate.checkpoint,
		SourceSequence:   sourceSequence,
		CompactedThrough: compactedThrough,
		Groups:           aggregate.groupCount,
		DistinctValues:   distinctValues,
		CompactionCount:  aggregate.compactionCount,
		EstimatedBytes:   estimateTypedTableAggregateBytes(aggregate),
	}
}

func typedTableAggregateTableState(table *TypedTable) (sourceSequence, compactedThrough uint64) {
	if table == nil {
		return 0, 0
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	return table.sequence, table.compactedThrough
}

func estimateTypedTableAggregateBytes(aggregate *TypedTableAggregate) uint64 {
	if aggregate == nil {
		return 0
	}
	var total uint64
	addEstimatedBytes(&total, uint64(len(aggregate.groups))*80)
	addEstimatedBytes(&total, uint64(len(aggregate.compactGroupOrder))*16)
	addEstimatedBytes(&total, uint64(len(aggregate.groupDictionaries))*64)
	for _, bucket := range aggregate.groups {
		addEstimatedBytes(&total, estimateTypedTableAggregateGroupBytes(bucket.group))
		for _, collision := range bucket.collisions {
			addEstimatedBytes(&total, estimateTypedTableAggregateGroupBytes(collision))
		}
	}
	return total
}

func estimateTypedTableAggregateGroupBytes(group typedTableAggregateGroup) uint64 {
	bytes := uint64(96 + len(group.key))
	bytes += uint64(len(group.values)) * 32
	bytes += uint64(len(group.codes)) * 4
	bytes += uint64(len(group.kinds))
	bytes += uint64(len(group.minValues)+len(group.maxValues)+len(group.distinctValues)) * 64
	return bytes
}

func addEstimatedBytes(total *uint64, value uint64) {
	if ^uint64(0)-*total < value {
		*total = ^uint64(0)
		return
	}
	*total += value
}
