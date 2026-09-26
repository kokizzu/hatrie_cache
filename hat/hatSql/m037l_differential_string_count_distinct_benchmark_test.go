package hatSql

import (
	"fmt"
	"testing"
)

var benchmarkM037lStringDistinctSink int

func benchmarkM037lStringDistinctRows() []DifferentialRow {
	const inserts = 1024
	rows := make([]DifferentialRow, 0, inserts*2)
	for index := 0; index < inserts; index++ {
		rows = append(rows, DifferentialRow{
			Key:  fmt.Sprintf("insert-%04d", index),
			Time: uint64(index + 1),
			Diff: 1,
			Row: Row{
				"group": fmt.Sprintf("group-%03d", index%128),
				"value": fmt.Sprintf("value-%03d", index%192),
			},
		})
	}
	for index := inserts - 1; index >= 0; index-- {
		original := rows[index]
		rows = append(rows, DifferentialRow{
			Key:  original.Key,
			Time: uint64(len(rows) + 1),
			Diff: -1,
			Row:  original.Row,
		})
	}
	return rows
}

func benchmarkM037lStringDistinctNaive(rows []DifferentialRow) []DifferentialRow {
	type visibleState struct {
		count    int64
		distinct int64
	}
	visible := make(map[string]visibleState)
	emitted := make([]DifferentialRow, 0, len(rows)*2)
	for index, update := range rows {
		key := update.Row["group"].(string)
		count := int64(0)
		values := make(map[string]int64)
		for _, prior := range rows[:index+1] {
			if prior.Row["group"].(string) != key {
				continue
			}
			count += prior.Diff
			value := prior.Row["value"].(string)
			values[value] += prior.Diff
		}
		distinct := int64(0)
		for _, multiplicity := range values {
			if multiplicity > 0 {
				distinct++
			}
		}
		previous, hadPrevious := visible[key]
		switch {
		case !hadPrevious && count > 0:
			emitted = append(emitted, DifferentialRow{Key: key, Time: update.Time, Diff: 1, Row: Row{"count_distinct": distinct}})
		case hadPrevious && count == 0:
			emitted = append(emitted, DifferentialRow{Key: key, Time: update.Time, Diff: -1, Row: Row{"count_distinct": previous.distinct}})
		case hadPrevious && previous.distinct != distinct:
			emitted = append(emitted,
				DifferentialRow{Key: key, Time: update.Time, Diff: -1, Row: Row{"count_distinct": previous.distinct}},
				DifferentialRow{Key: key, Time: update.Time, Diff: 1, Row: Row{"count_distinct": distinct}},
			)
		}
		if count == 0 {
			delete(visible, key)
		} else {
			visible[key] = visibleState{count: count, distinct: distinct}
		}
	}
	return emitted
}

func BenchmarkM037lDifferentialStringCountDistinct(b *testing.B) {
	rows := benchmarkM037lStringDistinctRows()
	b.Run("naive_rebuild", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			benchmarkM037lStringDistinctSink = len(benchmarkM037lStringDistinctNaive(rows))
		}
	})
	b.Run("incremental", func(b *testing.B) {
		key := func(row SQLRow) string {
			return row["group"].(string)
		}
		value := func(row SQLRow) (string, error) {
			return row["value"].(string), nil
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			result, err := GroupCountDistinctStringDifferentialRows(rows, key, value)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkM037lStringDistinctSink = len(result)
		}
	})
}
