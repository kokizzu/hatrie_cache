package hatSql

import (
	"fmt"
	"strconv"
	"testing"
)

var differentialGroupMinMaxBenchmarkSink int

func benchmarkNaiveGroupMinMax(rows []DifferentialRow, groupKey DifferentialGroupByKeyFunc, value DifferentialInt64ValueFunc) ([]DifferentialRow, error) {
	type state struct {
		values  map[int64]int64
		min     int64
		max     int64
		present bool
	}

	states := make(map[string]state, len(rows))
	emitted := make([]DifferentialRow, 0, len(rows)*2)
	for _, update := range rows {
		if update.Diff == 0 {
			continue
		}
		key := groupKey(update.Row)
		rowValue, err := value(update.Row)
		if err != nil {
			return nil, err
		}
		current := states[key]
		if current.values == nil {
			current.values = make(map[int64]int64)
		}
		nextCount, ok := addDifferentialCounts(sumValueCounts(current.values), update.Diff)
		if !ok || nextCount < 0 {
			return nil, fmt.Errorf("invalid benchmark count")
		}
		currentValueCount := current.values[rowValue]
		nextValueCount, ok := addDifferentialCounts(currentValueCount, update.Diff)
		if !ok || nextValueCount < 0 {
			return nil, fmt.Errorf("invalid benchmark value count")
		}
		if nextValueCount == 0 {
			delete(current.values, rowValue)
		} else {
			current.values[rowValue] = nextValueCount
		}

		previous := current
		if nextCount == 0 {
			delete(states, key)
		} else {
			current.present = true
			first := true
			for candidate := range current.values {
				if first || candidate < current.min {
					current.min = candidate
				}
				if first || candidate > current.max {
					current.max = candidate
				}
				first = false
			}
			states[key] = current
		}

		if !previous.present && nextCount > 0 {
			emitted = append(emitted, DifferentialRow{Key: key, Time: update.Time, Diff: 1, Row: Row{"min": current.min, "max": current.max}})
		} else if previous.present && nextCount == 0 {
			emitted = append(emitted, DifferentialRow{Key: key, Time: update.Time, Diff: -1, Row: Row{"min": previous.min, "max": previous.max}})
		} else if previous.present && (previous.min != current.min || previous.max != current.max) {
			emitted = append(emitted,
				DifferentialRow{Key: key, Time: update.Time, Diff: -1, Row: Row{"min": previous.min, "max": previous.max}},
				DifferentialRow{Key: key, Time: update.Time, Diff: 1, Row: Row{"min": current.min, "max": current.max}},
			)
		}
	}
	if len(emitted) == 0 {
		return nil, nil
	}
	return emitted, nil
}

func sumValueCounts(values map[int64]int64) int64 {
	var total int64
	for _, count := range values {
		total += count
	}
	return total
}

func differentialGroupMinMaxBenchmarkRows() []DifferentialRow {
	rows := make([]DifferentialRow, 2048)
	for index := range rows {
		rows[index] = DifferentialRow{
			Key:  fmt.Sprintf("row-%d", index),
			Time: uint64(index),
			Diff: 1,
			Row: Row{
				"group": strconv.Itoa(index % 256),
				"value": int64((index * 17) % 1024),
			},
		}
	}
	return rows
}

func BenchmarkDifferentialGroupMinMax(b *testing.B) {
	rows := differentialGroupMinMaxBenchmarkRows()
	key := func(row SQLRow) string { return row["group"].(string) }
	value := func(row SQLRow) (int64, error) { return row["value"].(int64), nil }

	b.Run("naive_rebuild", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			result, err := benchmarkNaiveGroupMinMax(rows, key, value)
			if err != nil {
				b.Fatal(err)
			}
			differentialGroupMinMaxBenchmarkSink += len(result)
		}
	})

	b.Run("incremental", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			result, err := GroupMinMaxInt64DifferentialRows(rows, key, value)
			if err != nil {
				b.Fatal(err)
			}
			differentialGroupMinMaxBenchmarkSink += len(result)
		}
	})
}
