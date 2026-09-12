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

// Stats computes exact row, NULL, value, and supported scalar min/max counts
// for the current active rows. It is read-only and safe to call concurrently
// with table readers and writers.
func (table *TypedTable) Stats() TypedTableStats {
	if table == nil {
		return TypedTableStats{}
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
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
