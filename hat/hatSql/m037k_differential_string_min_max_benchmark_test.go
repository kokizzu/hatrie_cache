package hatSql

import (
	"fmt"
	"strconv"
	"testing"
)

var differentialStringMinMaxBenchmarkSink int

func benchmarkNaiveGroupMinMaxString(rows []DifferentialRow, groupKey DifferentialGroupByKeyFunc, value DifferentialStringValueFunc) ([]DifferentialRow, error) {
	type state struct {
		values  map[string]int64
		min     string
		max     string
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
			current.values = make(map[string]int64)
		}
		nextCount, ok := addDifferentialCounts(sumStringValueCounts(current.values), update.Diff)
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

func sumStringValueCounts(values map[string]int64) int64 {
	var total int64
	for _, count := range values {
		total += count
	}
	return total
}

func differentialStringMinMaxBenchmarkRows() []DifferentialRow {
	rows := make([]DifferentialRow, 2048)
	for index := range rows {
		rows[index] = DifferentialRow{
			Key:  fmt.Sprintf("row-%d", index),
			Time: uint64(index),
			Diff: 1,
			Row: Row{
				"group": strconv.Itoa(index % 256),
				"value": "value-" + strconv.Itoa((index*17)%1024),
			},
		}
	}
	return rows
}

func BenchmarkM037kDifferentialStringMinMax(b *testing.B) {
	rows := differentialStringMinMaxBenchmarkRows()
	key := func(row SQLRow) string { return row["group"].(string) }
	value := func(row SQLRow) (string, error) { return row["value"].(string), nil }

	b.Run("naive_rebuild", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			result, err := benchmarkNaiveGroupMinMaxString(rows, key, value)
			if err != nil {
				b.Fatal(err)
			}
			differentialStringMinMaxBenchmarkSink += len(result)
		}
	})

	b.Run("incremental", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			result, err := GroupMinMaxStringDifferentialRows(rows, key, value)
			if err != nil {
				b.Fatal(err)
			}
			differentialStringMinMaxBenchmarkSink += len(result)
		}
	})
}
