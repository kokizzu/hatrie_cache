package hatSql

import (
	"fmt"
	"testing"
)

var m065yMutableRangeNthValueBenchmarkSink interface{}

func BenchmarkM065yMutableRangeNthValueBaseline(b *testing.B) {
	rows := m065yMutableRangeNthValueBenchmarkRows(10000)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		rows[5000]["value"] = int64(index)
		m065yMutableRangeNthValueBenchmarkSink = naiveM065yMutableRangeNthValueOutputs(rows, 64, 256)
	}
}

func BenchmarkM065yMutableRangeNthValue(b *testing.B) {
	window, err := NewMutableIncrementalRangeNthValueWindow(IncrementalRangeNthValueWindowDefinition{
		Position:       64,
		OutputColumn:   "result",
		FramePreceding: 256,
		OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
		ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
	})
	if err != nil {
		b.Fatal(err)
	}
	rows := m065yMutableRangeNthValueBenchmarkRows(10000)
	mutations := make([]IncrementalRangeNthValueWindowMutation, 0, len(rows))
	for _, row := range rows {
		mutations = append(mutations, IncrementalRangeNthValueWindowMutation{
			Operation: IncrementalRangeNthValueWindowInsert,
			Row:       row,
		})
	}
	if _, err := window.Apply(mutations); err != nil {
		b.Fatal(err)
	}
	update := Row{"id": "row-5000", "order": int64(5000), "value": int64(0)}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		update["value"] = int64(index)
		if _, err := window.Apply([]IncrementalRangeNthValueWindowMutation{{
			Operation: IncrementalRangeNthValueWindowUpdate,
			Key:       "row-5000",
			Row:       update,
		}}); err != nil {
			b.Fatal(err)
		}
	}
}

func m065yMutableRangeNthValueBenchmarkRows(count int) []Row {
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

func naiveM065yMutableRangeNthValueOutputs(rows []Row, position int, preceding int64) map[string]Row {
	outputs := make(map[string]Row, len(rows))
	for index, row := range rows {
		order := row["order"].(int64)
		first := index
		for first > 0 && rows[first-1]["order"].(int64) >= order-preceding {
			first--
		}
		value := interface{}(nil)
		valueIndex := first + position - 1
		if valueIndex <= index {
			value = rows[valueIndex]["value"]
		}
		output := cloneMutableIncrementalRangeNthValueWindowRow(row)
		output["result"] = value
		outputs[row["id"].(string)] = output
	}
	return outputs
}
