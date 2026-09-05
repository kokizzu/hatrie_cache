package hatSql

import (
	"fmt"
	"math"
)

// DifferentialRow is one weighted row update. Key is the caller-provided
// stable identity of Row; Time separates updates from different logical
// frontiers, and Diff may be positive or negative.
type DifferentialRow struct {
	Key  string
	Time uint64
	Diff int64
	Row  Row
}

type differentialRowKey struct {
	key  string
	time uint64
}

// ConsolidateDifferentialRows combines entries with the same Key and Time,
// preserving negative updates and duplicate multiplicity in Diff. The input
// slice and its row maps are not mutated. Results retain the first surviving
// key order and zero-sum entries are removed.
func ConsolidateDifferentialRows(rows []DifferentialRow) ([]DifferentialRow, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	indexes := make(map[differentialRowKey]int, len(rows))
	consolidated := make([]DifferentialRow, 0, len(rows))
	for _, entry := range rows {
		if entry.Key == "" {
			return nil, fmt.Errorf("differential row key is required")
		}
		if entry.Diff == 0 {
			continue
		}
		key := differentialRowKey{key: entry.Key, time: entry.Time}
		outputIndex, exists := indexes[key]
		if !exists {
			entry.Row = cloneDifferentialRow(entry.Row)
			indexes[key] = len(consolidated)
			consolidated = append(consolidated, entry)
			continue
		}
		combined, ok := addDifferentialCounts(consolidated[outputIndex].Diff, entry.Diff)
		if !ok {
			return nil, fmt.Errorf("differential row %q at time %d overflows diff", entry.Key, entry.Time)
		}
		consolidated[outputIndex].Diff = combined
		if combined == 0 {
			delete(indexes, key)
		}
	}
	result := consolidated[:0]
	for _, entry := range consolidated {
		if entry.Diff != 0 {
			result = append(result, entry)
		}
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

func cloneDifferentialRow(row Row) Row {
	if row == nil {
		return nil
	}
	cloned := make(Row, len(row))
	for key, value := range row {
		switch value := value.(type) {
		case []byte:
			clonedValue := make([]byte, len(value))
			copy(clonedValue, value)
			cloned[key] = clonedValue
		default:
			cloned[key] = value
		}
	}
	return cloned
}

func addDifferentialCounts(left, right int64) (int64, bool) {
	if right > 0 && left > math.MaxInt64-right {
		return 0, false
	}
	if right < 0 && left < math.MinInt64-right {
		return 0, false
	}
	return left + right, true
}
