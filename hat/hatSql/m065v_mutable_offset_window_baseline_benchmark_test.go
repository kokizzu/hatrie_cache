package hatSql

import (
	"strconv"
	"testing"
)

func BenchmarkM065vMutableOffsetWindowBaseline(b *testing.B) {
	definition := IncrementalOffsetWindowDefinition{
		Direction:    IncrementalWindowLag,
		OutputColumn: "lag_value",
		Offset:       2,
		DefaultValue: int64(-1),
		PartitionKey: func(row Row) (string, error) { return row["partition"].(string), nil },
		OrderKey:     func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:       func(row Row) (string, error) { return row["id"].(string), nil },
		ValueKey:     func(row Row) (interface{}, error) { return row["value"], nil },
	}
	rows := make(map[string]Row, 10000)
	for index := 0; index < 10000; index++ {
		key := "row-" + strconv.Itoa(index)
		rows[key] = Row{"id": key, "partition": "p", "order": int64(index), "value": int64(index)}
	}
	updated := cloneIncrementalOffsetBenchmarkRows(rows)
	updated["row-5000"] = Row{"id": "row-5000", "partition": "p", "order": int64(5000), "value": int64(-5000)}
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		if iteration%2 == 0 {
			_ = naiveOffsetWindowOutputs(updated, definition)
		} else {
			_ = naiveOffsetWindowOutputs(rows, definition)
		}
	}
}

func BenchmarkM065vMutableOffsetWindow(b *testing.B) {
	definition := IncrementalOffsetWindowDefinition{
		Direction:    IncrementalWindowLag,
		OutputColumn: "lag_value",
		Offset:       2,
		DefaultValue: int64(-1),
		PartitionKey: func(row Row) (string, error) { return row["partition"].(string), nil },
		OrderKey:     func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:       func(row Row) (string, error) { return row["id"].(string), nil },
		ValueKey:     func(row Row) (interface{}, error) { return row["value"], nil },
	}
	window, err := NewMutableIncrementalOffsetWindow(definition)
	if err != nil {
		b.Fatal(err)
	}
	inserts := make([]IncrementalOffsetWindowMutation, 10000)
	for index := range inserts {
		key := "row-" + strconv.Itoa(index)
		inserts[index] = IncrementalOffsetWindowMutation{
			Operation: IncrementalOffsetWindowInsert,
			Row:       Row{"id": key, "partition": "p", "order": int64(index), "value": int64(index)},
		}
	}
	if _, err := window.Apply(inserts); err != nil {
		b.Fatal(err)
	}
	mutations := [2][]IncrementalOffsetWindowMutation{
		{{Operation: IncrementalOffsetWindowUpdate, Key: "row-5000", Row: Row{"id": "row-5000", "partition": "p", "order": int64(5000), "value": int64(-5000)}}},
		{{Operation: IncrementalOffsetWindowUpdate, Key: "row-5000", Row: Row{"id": "row-5000", "partition": "p", "order": int64(5000), "value": int64(5000)}}},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if _, err := window.Apply(mutations[iteration%len(mutations)]); err != nil {
			b.Fatal(err)
		}
	}
}

func cloneIncrementalOffsetBenchmarkRows(rows map[string]Row) map[string]Row {
	clone := make(map[string]Row, len(rows))
	for key, row := range rows {
		clone[key] = cloneIncrementalOffsetWindowRow(row)
	}
	return clone
}
