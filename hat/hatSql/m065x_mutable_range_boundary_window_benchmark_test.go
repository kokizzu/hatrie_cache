package hatSql

import (
	"fmt"
	"testing"
)

var m065xMutableRangeBoundaryBenchmarkSink interface{}

func BenchmarkM065xMutableRangeBoundaryBaseline(b *testing.B) {
	for _, kind := range []IncrementalRangeBoundaryWindowKind{IncrementalRangeFirstValue, IncrementalRangeLastValue} {
		b.Run(fmt.Sprintf("kind-%d", kind), func(b *testing.B) {
			rows := m065xMutableRangeBoundaryBenchmarkRows(2000)
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				rows[1000]["value"] = int64(index)
				m065xMutableRangeBoundaryBenchmarkSink = naiveM065xMutableRangeBoundaryOutputs(rows, kind, 64)
			}
		})
	}
}

func BenchmarkM065xMutableRangeBoundary(b *testing.B) {
	for _, kind := range []IncrementalRangeBoundaryWindowKind{IncrementalRangeFirstValue, IncrementalRangeLastValue} {
		b.Run(fmt.Sprintf("kind-%d", kind), func(b *testing.B) {
			window, err := NewMutableIncrementalRangeBoundaryWindow(IncrementalRangeBoundaryWindowDefinition{
				Kind:           kind,
				OutputColumn:   "result",
				FramePreceding: 64,
				OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
				RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
				ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
			})
			if err != nil {
				b.Fatal(err)
			}
			rows := m065xMutableRangeBoundaryBenchmarkRows(2000)
			mutations := make([]IncrementalRangeBoundaryWindowMutation, 0, len(rows))
			for _, row := range rows {
				mutations = append(mutations, IncrementalRangeBoundaryWindowMutation{
					Operation: IncrementalRangeBoundaryWindowInsert,
					Row:       row,
				})
			}
			if _, err := window.Apply(mutations); err != nil {
				b.Fatal(err)
			}
			update := Row{"id": "row-1000", "order": int64(1000), "value": int64(0)}
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				update["value"] = int64(index)
				if _, err := window.Apply([]IncrementalRangeBoundaryWindowMutation{{
					Operation: IncrementalRangeBoundaryWindowUpdate,
					Key:       "row-1000",
					Row:       update,
				}}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkM065acMutableRangeBoundaryBatchedSamePosition(b *testing.B) {
	for _, kind := range []IncrementalRangeBoundaryWindowKind{IncrementalRangeFirstValue, IncrementalRangeLastValue} {
		b.Run(fmt.Sprintf("kind-%d", kind), func(b *testing.B) {
			window, err := NewMutableIncrementalRangeBoundaryWindow(IncrementalRangeBoundaryWindowDefinition{
				Kind:           kind,
				OutputColumn:   "result",
				FramePreceding: 64,
				OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
				RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
				ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
			})
			if err != nil {
				b.Fatal(err)
			}
			rows := m065xMutableRangeBoundaryBenchmarkRows(2000)
			inserts := make([]IncrementalRangeBoundaryWindowMutation, 0, len(rows))
			for _, row := range rows {
				inserts = append(inserts, IncrementalRangeBoundaryWindowMutation{
					Operation: IncrementalRangeBoundaryWindowInsert,
					Row:       row,
				})
			}
			if _, err := window.Apply(inserts); err != nil {
				b.Fatal(err)
			}
			batches := [2][]IncrementalRangeBoundaryWindowMutation{
				{
					{Operation: IncrementalRangeBoundaryWindowUpdate, Key: "row-1000", Row: Row{"id": "row-1000", "order": int64(1000), "value": int64(-1000)}},
					{Operation: IncrementalRangeBoundaryWindowUpdate, Key: "row-1001", Row: Row{"id": "row-1001", "order": int64(1001), "value": int64(-1001)}},
				},
				{
					{Operation: IncrementalRangeBoundaryWindowUpdate, Key: "row-1000", Row: Row{"id": "row-1000", "order": int64(1000), "value": int64(1000)}},
					{Operation: IncrementalRangeBoundaryWindowUpdate, Key: "row-1001", Row: Row{"id": "row-1001", "order": int64(1001), "value": int64(1001)}},
				},
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if _, err := window.Apply(batches[iteration%len(batches)]); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func m065xMutableRangeBoundaryBenchmarkRows(count int) []Row {
	rows := make([]Row, count)
	for index := range rows {
		rows[index] = Row{
			"id":    fmt.Sprintf("row-%d", index),
			"order": int64(index),
			"value": int64(index),
		}
	}
	return rows
}

func naiveM065xMutableRangeBoundaryOutputs(rows []Row, kind IncrementalRangeBoundaryWindowKind, preceding int64) map[string]Row {
	outputs := make(map[string]Row, len(rows))
	for index, row := range rows {
		order := row["order"].(int64)
		first := index
		for first > 0 && rows[first-1]["order"].(int64) >= order-preceding {
			first--
		}
		value := rows[index]["value"]
		if kind == IncrementalRangeFirstValue {
			value = rows[first]["value"]
		}
		output := cloneMutableIncrementalRangeBoundaryWindowRow(row)
		output["result"] = value
		outputs[row["id"].(string)] = output
	}
	return outputs
}
