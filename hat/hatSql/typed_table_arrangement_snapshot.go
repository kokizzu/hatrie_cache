package hatSql

import "sort"

// TypedTableAggregateArrangementInfo describes one live aggregate definition
// owned by a TypedTableAggregateArrangements registry. References is the
// number of active leases sharing the same aggregate state.
type TypedTableAggregateArrangementInfo struct {
	TableName      string                        `json:"table_name"`
	Definition     TypedTableAggregateDefinition `json:"definition"`
	References     int                           `json:"references"`
	Shared         bool                          `json:"shared"`
	Checkpoint     uint64                        `json:"checkpoint"`
	SourceSequence uint64                        `json:"source_sequence"`
	Stale          bool                          `json:"stale"`
}

// Snapshot returns a deterministic, independent view of every live aggregate
// arrangement. The registry owns the shared state; References and Shared make
// reuse visible without exposing mutable internals. A nil registry returns nil
// and an empty initialized registry returns an empty slice.
func (arrangements *TypedTableAggregateArrangements) Snapshot() []TypedTableAggregateArrangementInfo {
	if arrangements == nil {
		return nil
	}
	arrangements.mu.Lock()
	defer arrangements.mu.Unlock()
	keys := make([]string, 0, len(arrangements.entries))
	for key := range arrangements.entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	snapshot := make([]TypedTableAggregateArrangementInfo, 0, len(keys))
	for _, key := range keys {
		entry := arrangements.entries[key]
		if entry == nil || entry.aggregate == nil {
			continue
		}
		entry.mu.Lock()
		aggregate := entry.aggregate
		definition, tableName, sourceSequence := typedTableAggregateArrangementSnapshot(aggregate)
		checkpoint := aggregate.checkpoint
		references := entry.references
		entry.mu.Unlock()
		snapshot = append(snapshot, TypedTableAggregateArrangementInfo{
			TableName:      tableName,
			Definition:     definition,
			References:     references,
			Shared:         references > 1,
			Checkpoint:     checkpoint,
			SourceSequence: sourceSequence,
			Stale:          checkpoint < sourceSequence,
		})
	}
	return snapshot
}

func typedTableAggregateArrangementSnapshot(aggregate *TypedTableAggregate) (TypedTableAggregateDefinition, string, uint64) {
	if aggregate == nil || aggregate.table == nil {
		return TypedTableAggregateDefinition{}, "", 0
	}
	table := aggregate.table
	table.mu.RLock()
	defer table.mu.RUnlock()
	definition := TypedTableAggregateDefinition{}
	for _, index := range aggregate.groupBy {
		if index >= 0 && index < len(table.schema.Columns) {
			definition.GroupBy = append(definition.GroupBy, table.schema.Columns[index].Name)
		}
	}
	definition.SumField = typedTableColumnName(table.schema.Columns, aggregate.sumField)
	definition.MinField = typedTableColumnName(table.schema.Columns, aggregate.minField)
	definition.MaxField = typedTableColumnName(table.schema.Columns, aggregate.maxField)
	definition.DistinctField = typedTableColumnName(table.schema.Columns, aggregate.distinctField)
	return definition, table.schema.Name, table.sequence
}

func typedTableColumnName(columns []TypedTableColumn, index int) string {
	if index < 0 || index >= len(columns) {
		return ""
	}
	return columns[index].Name
}

// TypedTableJoinArrangementInfo describes one live join definition owned by a
// TypedTableJoinArrangements registry. References is the number of active
// leases sharing the same join state.
type TypedTableJoinArrangementInfo struct {
	LeftTableName       string                   `json:"left_table_name"`
	RightTableName      string                   `json:"right_table_name"`
	Definition          TypedTableJoinDefinition `json:"definition"`
	References          int                      `json:"references"`
	Shared              bool                     `json:"shared"`
	LeftCheckpoint      uint64                   `json:"left_checkpoint"`
	LeftSourceSequence  uint64                   `json:"left_source_sequence"`
	RightCheckpoint     uint64                   `json:"right_checkpoint"`
	RightSourceSequence uint64                   `json:"right_source_sequence"`
	Stale               bool                     `json:"stale"`
}

// Snapshot returns a deterministic, independent view of every live join
// arrangement. It reports both input checkpoints and source sequences so a
// caller can distinguish reusable state from reusable but stale state.
func (arrangements *TypedTableJoinArrangements) Snapshot() []TypedTableJoinArrangementInfo {
	if arrangements == nil {
		return nil
	}
	arrangements.mu.Lock()
	defer arrangements.mu.Unlock()
	keys := make([]string, 0, len(arrangements.entries))
	for key := range arrangements.entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	snapshot := make([]TypedTableJoinArrangementInfo, 0, len(keys))
	for _, key := range keys {
		entry := arrangements.entries[key]
		if entry == nil || entry.join == nil {
			continue
		}
		entry.mu.Lock()
		join := entry.join
		join.mu.RLock()
		leftTableName, leftSourceSequence := typedTableNameAndSequence(join.left)
		rightTableName, rightSourceSequence := typedTableNameAndSequence(join.right)
		leftCheckpoint := join.leftCheckpoint
		rightCheckpoint := join.rightCheckpoint
		definition := join.definition
		join.mu.RUnlock()
		references := entry.refs
		entry.mu.Unlock()
		snapshot = append(snapshot, TypedTableJoinArrangementInfo{
			LeftTableName:       leftTableName,
			RightTableName:      rightTableName,
			Definition:          definition,
			References:          references,
			Shared:              references > 1,
			LeftCheckpoint:      leftCheckpoint,
			LeftSourceSequence:  leftSourceSequence,
			RightCheckpoint:     rightCheckpoint,
			RightSourceSequence: rightSourceSequence,
			Stale:               leftCheckpoint < leftSourceSequence || rightCheckpoint < rightSourceSequence,
		})
	}
	return snapshot
}

func typedTableNameAndSequence(table *TypedTable) (string, uint64) {
	if table == nil {
		return "", 0
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	return table.schema.Name, table.sequence
}
