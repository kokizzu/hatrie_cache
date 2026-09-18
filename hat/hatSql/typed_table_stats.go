package hatSql

import (
	"math"
	"time"
)

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
// the current active rows. With TTL enabled, the snapshot is recomputed so
// elapsed time cannot leave a stale cached row count. Otherwise it is cached
// until a row mutation. The result is read-only and safe to call concurrently
// with table readers and writers.
func (table *TypedTable) Stats() TypedTableStats {
	if table == nil {
		return TypedTableStats{}
	}
	table.mu.RLock()
	if table.ttl == nil && table.columnTTLs == nil && table.statsCacheValid {
		stats := cloneTypedTableStats(table.statsCache)
		table.mu.RUnlock()
		return stats
	}
	table.mu.RUnlock()

	table.mu.Lock()
	defer table.mu.Unlock()
	if table.ttl == nil && table.columnTTLs == nil && table.statsCacheValid {
		return cloneTypedTableStats(table.statsCache)
	}
	stats := table.computeStatsLocked()
	if table.ttl == nil && table.columnTTLs == nil {
		table.statsCache = stats
		table.statsCacheValid = true
	}
	return cloneTypedTableStats(stats)
}

func (table *TypedTable) computeStatsLocked() TypedTableStats {
	stats := TypedTableStats{Columns: make([]TypedTableColumnStats, len(table.schema.Columns))}
	for index, column := range table.schema.Columns {
		stats.Columns[index] = TypedTableColumnStats{Name: column.Name, Kind: column.Kind}
	}
	now := time.Time{}
	if table.ttl != nil {
		now = table.typedTableTTLNow()
	}
	for row := range table.keys {
		if table.ttl != nil {
			if table.typedTableRowHiddenLocked(row, now) {
				continue
			}
		} else if table.typedTableRowDeletedLocked(row) {
			continue
		}
		stats.RowCount++
		for index, storage := range table.columns {
			columnStats := &stats.Columns[index]
			if !storage.valid[row] || (table.columnTTLs != nil && table.typedTableColumnExpiredLocked(index, row)) {
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

func (table *TypedTable) invalidateTypedTableDerivedCachesLocked() {
	table.statsCacheValid = false
	table.histogramCache = nil
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
