package hatSql

import (
	"sort"
	"strconv"
	"testing"
)

func BenchmarkM065wMutableRangeWindowBaseline(b *testing.B) {
	definition := m065wRangeBenchmarkDefinition()
	rows := m065wRangeBenchmarkRows(2000)
	updated := cloneM065wRangeBenchmarkRows(rows)
	updated["row-1000"] = Row{"id": "row-1000", "partition": "p", "order": int64(1000), "value": int64(-1000)}
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		if iteration%2 == 0 {
			_ = naiveM065wRangeOutputs(updated, definition)
		} else {
			_ = naiveM065wRangeOutputs(rows, definition)
		}
	}
}

func BenchmarkM065wMutableRangeWindow(b *testing.B) {
	definition := m065wRangeBenchmarkDefinition()
	window, err := NewMutableIncrementalRangeWindow(definition)
	if err != nil {
		b.Fatal(err)
	}
	inserts := make([]IncrementalRangeWindowMutation, 2000)
	for index := range inserts {
		key := "row-" + strconv.Itoa(index)
		inserts[index] = IncrementalRangeWindowMutation{
			Operation: IncrementalRangeWindowInsert,
			Row:       Row{"id": key, "partition": "p", "order": int64(index), "value": int64(index)},
		}
	}
	if _, err := window.Apply(inserts); err != nil {
		b.Fatal(err)
	}
	mutations := [2][]IncrementalRangeWindowMutation{
		{{Operation: IncrementalRangeWindowUpdate, Key: "row-1000", Row: Row{"id": "row-1000", "partition": "p", "order": int64(1000), "value": int64(-1000)}}},
		{{Operation: IncrementalRangeWindowUpdate, Key: "row-1000", Row: Row{"id": "row-1000", "partition": "p", "order": int64(1000), "value": int64(1000)}}},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if _, err := window.Apply(mutations[iteration%len(mutations)]); err != nil {
			b.Fatal(err)
		}
	}
}

func m065wRangeBenchmarkDefinition() IncrementalRangeWindowDefinition {
	return IncrementalRangeWindowDefinition{
		Kind:           IncrementalRangeWindowSumInt64,
		OutputColumn:   "range_sum",
		FramePreceding: 64,
		OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
		ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
	}
}

func m065wRangeBenchmarkRows(count int) map[string]Row {
	rows := make(map[string]Row, count)
	for index := 0; index < count; index++ {
		key := "row-" + strconv.Itoa(index)
		rows[key] = Row{"id": key, "partition": "p", "order": int64(index), "value": int64(index)}
	}
	return rows
}

func cloneM065wRangeBenchmarkRows(rows map[string]Row) map[string]Row {
	clone := make(map[string]Row, len(rows))
	for key, row := range rows {
		clone[key] = cloneIncrementalRangeWindowRow(row)
	}
	return clone
}

func naiveM065wRangeOutputs(rows map[string]Row, definition IncrementalRangeWindowDefinition) map[string]Row {
	ordered := make([]Row, 0, len(rows))
	for _, row := range rows {
		ordered = append(ordered, row)
	}
	sort.Slice(ordered, func(left, right int) bool {
		return ordered[left]["order"].(int64) < ordered[right]["order"].(int64)
	})
	outputs := make(map[string]Row, len(ordered))
	for _, row := range ordered {
		order := row["order"].(int64)
		var sum int64
		for _, candidate := range ordered {
			candidateOrder := candidate["order"].(int64)
			if candidateOrder < order-definition.FramePreceding || candidateOrder > order {
				continue
			}
			sum += candidate["value"].(int64)
		}
		outputs[row["id"].(string)] = incrementalRangeWindowOutput(row, definition.OutputColumn, sum)
	}
	return outputs
}
