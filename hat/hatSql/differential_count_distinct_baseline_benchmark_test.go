package hatSql

import "testing"

func BenchmarkM037jCountDistinctRebuildBaseline(b *testing.B) {
	rows := benchmarkCountDistinctRows()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := rebuildCountDistinctBenchmark(rows); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM037jCountDistinctIncremental(b *testing.B) {
	rows := benchmarkCountDistinctRows()
	key := func(row SQLRow) string {
		return row["group"].(string)
	}
	value := func(row SQLRow) (int64, error) {
		return row["value"].(int64), nil
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := GroupCountDistinctInt64DifferentialRows(rows, key, value); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkCountDistinctRows() []DifferentialRow {
	rows := make([]DifferentialRow, 4096)
	for index := range rows {
		rows[index] = DifferentialRow{
			Key:  "row",
			Time: uint64(index),
			Diff: 1,
			Row: Row{
				"group": "group-" + string(rune('a'+index%64)),
				"value": int64(index % 32),
			},
		}
	}
	for index := range rows[:1024] {
		rows = append(rows, DifferentialRow{
			Key:  "row",
			Time: uint64(len(rows)),
			Diff: -1,
			Row:  rows[index].Row,
		})
	}
	return rows
}

func rebuildCountDistinctBenchmark(rows []DifferentialRow) error {
	type groupState struct {
		count    int64
		values   map[int64]int64
		distinct int64
	}
	groups := make(map[string]groupState, 64)
	emitted := make([]DifferentialRow, 0, len(rows)*2)
	for _, update := range rows {
		if update.Diff == 0 {
			continue
		}
		key := update.Row["group"].(string)
		value := update.Row["value"].(int64)
		state := groups[key]
		if state.values == nil {
			state.values = make(map[int64]int64)
		}
		previous := state
		nextCount := state.count + update.Diff
		nextValueCount := state.values[value] + update.Diff
		if nextCount < 0 || nextValueCount < 0 {
			return ErrDifferentialGroupByNegativeCount
		}
		state.count = nextCount
		state.values[value] = nextValueCount
		var distinct int64
		for distinctValue, multiplicity := range state.values {
			if multiplicity == 0 {
				delete(state.values, distinctValue)
				continue
			}
			distinct++
		}
		state.distinct = distinct
		switch {
		case previous.count == 0 && nextCount > 0:
			emitted = append(emitted, DifferentialRow{Key: key, Time: update.Time, Diff: 1, Row: Row{"count_distinct": distinct}})
		case previous.count > 0 && nextCount == 0:
			emitted = append(emitted, DifferentialRow{Key: key, Time: update.Time, Diff: -1, Row: Row{"count_distinct": previous.distinct}})
		case previous.distinct != distinct:
			emitted = append(emitted,
				DifferentialRow{Key: key, Time: update.Time, Diff: -1, Row: Row{"count_distinct": previous.distinct}},
				DifferentialRow{Key: key, Time: update.Time, Diff: 1, Row: Row{"count_distinct": distinct}},
			)
		}
		if nextCount == 0 {
			delete(groups, key)
		} else {
			groups[key] = state
		}
	}
	_ = emitted
	return nil
}
