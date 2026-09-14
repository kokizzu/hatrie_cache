package hatSql

import "math"

// TypedTableColumnStats reports exact statistics for one active typed-table
// column. NULL values contribute to NullCount but never to min/max. Float NaN
// values contribute to ValueCount and are excluded from min/max.
type TypedTableColumnStats struct {
	Name       string
	Kind       TypedTableKind
	NullCount  int
	ValueCount int
	HasMinMax  bool
	Min        TypedTableValue
	Max        TypedTableValue
}

// TypedTableStats is a schema-ordered snapshot of active table rows. The
// snapshot is computed from the existing typed column storage and retains no
// references to the table.
type TypedTableStats struct {
	RowCount int
	Columns  []TypedTableColumnStats
}

// Stats returns exact row, NULL, value, and supported scalar min/max counts for
// the current active rows. The snapshot is cached until a row mutation and is
// read-only and safe to call concurrently with table readers and writers.
func (table *TypedTable) Stats() TypedTableStats {
	if table == nil {
		return TypedTableStats{}
	}
	table.mu.RLock()
	if table.statsCacheValid {
		stats := cloneTypedTableStats(table.statsCache)
		table.mu.RUnlock()
		return stats
	}
	table.mu.RUnlock()

	table.mu.Lock()
	defer table.mu.Unlock()
	if table.statsCacheValid {
		return cloneTypedTableStats(table.statsCache)
	}
	stats := table.computeStatsLocked()
	table.statsCache = stats
	table.statsCacheValid = true
	return cloneTypedTableStats(stats)
}

func (table *TypedTable) computeStatsLocked() TypedTableStats {
	stats := TypedTableStats{Columns: make([]TypedTableColumnStats, len(table.schema.Columns))}
	for index, column := range table.schema.Columns {
		stats.Columns[index] = TypedTableColumnStats{Name: column.Name, Kind: column.Kind}
	}
	for row := range table.keys {
		if table.typedTableRowDeletedLocked(row) {
			continue
		}
		stats.RowCount++
		for index, storage := range table.columns {
			columnStats := &stats.Columns[index]
			if !storage.valid[row] {
				columnStats.NullCount++
				continue
			}
			columnStats.ValueCount++
			value := storage.value(row)
			if storage.kind == TypedTableFloat64 && math.IsNaN(value.Float64) {
				continue
			}
			if !columnStats.HasMinMax {
				columnStats.HasMinMax = true
				columnStats.Min = value
				columnStats.Max = value
				continue
			}
			if typedTableStatsLess(value, columnStats.Min) {
				columnStats.Min = value
			}
			if typedTableStatsLess(columnStats.Max, value) {
				columnStats.Max = value
			}
		}
	}
	return stats
}

func cloneTypedTableStats(stats TypedTableStats) TypedTableStats {
	if stats.Columns == nil {
		return stats
	}
	stats.Columns = append([]TypedTableColumnStats(nil), stats.Columns...)
	return stats
}

func typedTableStatsLess(left, right TypedTableValue) bool {
	switch left.Kind {
	case TypedTableString:
		return left.String < right.String
	case TypedTableInt64:
		return left.Int64 < right.Int64
	case TypedTableFloat64:
		return left.Float64 < right.Float64
	case TypedTableBool:
		return !left.Bool && right.Bool
	default:
		return false
	}
}
