package hatSql

import (
	"strconv"
	"testing"
)

var (
	benchmarkIncrementalNthValueResult     []DifferentialRow
	benchmarkIncrementalNthValueFullResult []DifferentialRow
)

func BenchmarkIncrementalNthValueWindow(b *testing.B) {
	seed := benchmarkIncrementalNthValueRows(1024)
	allRows := append(append([]Row(nil), seed...), Row{
		"id":        "tail-0",
		"partition": "p0",
		"order":     int64(64),
		"value":     "tail",
	})
	b.Run("full_scan", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkIncrementalNthValueFullResult = benchmarkRecomputeNthValueWindow(allRows, 4)
		}
	})
	b.Run("incremental", func(b *testing.B) {
		window := benchmarkNewIncrementalNthValueWindow(b, 4)
		if _, err := window.Append(seed); err != nil {
			b.Fatal(err)
		}
		tailIDs := benchmarkIncrementalNthValueTailIDs(b.N)
		tail := Row{"partition": "p0", "order": int64(64), "value": "tail"}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tail["id"] = tailIDs[i]
			tail["order"] = int64(64 + i)
			updates, err := window.Append([]Row{tail})
			if err != nil {
				b.Fatal(err)
			}
			benchmarkIncrementalNthValueResult = updates
		}
	})
}

func benchmarkIncrementalNthValueRows(count int) []Row {
	rows := make([]Row, count)
	for index := range rows {
		rows[index] = Row{
			"id":        "seed-" + strconv.Itoa(index),
			"partition": "p" + strconv.Itoa(index%16),
			"order":     int64(index / 16),
			"value":     "value-" + strconv.Itoa(index),
		}
	}
	return rows
}

func benchmarkIncrementalNthValueTailIDs(count int) []string {
	ids := make([]string, count)
	for index := range ids {
		ids[index] = "tail-" + strconv.Itoa(index)
	}
	return ids
}

func benchmarkNewIncrementalNthValueWindow(b *testing.B, position int) *IncrementalNthValueWindow {
	b.Helper()
	window, err := NewIncrementalNthValueWindow(IncrementalNthValueWindowDefinition{
		Position:     position,
		OutputColumn: "nth_value",
		PartitionKey: func(row Row) (string, error) { return row["partition"].(string), nil },
		OrderKey:     func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:       func(row Row) (string, error) { return row["id"].(string), nil },
		ValueKey:     func(row Row) (interface{}, error) { return row["value"], nil },
	})
	if err != nil {
		b.Fatal(err)
	}
	return window
}

func benchmarkRecomputeNthValueWindow(rows []Row, position int) []DifferentialRow {
	counts := make(map[string]int, 16)
	values := make(map[string]interface{}, 16)
	updates := make([]DifferentialRow, 0, len(rows))
	for _, row := range rows {
		partition := row["partition"].(string)
		counts[partition]++
		if counts[partition] == position {
			values[partition] = row["value"]
		}
		value := values[partition]
		updates = append(updates, DifferentialRow{
			Key:  row["id"].(string),
			Diff: 1,
			Row:  incrementalFrameWindowOutput(row, "nth_value", value),
		})
	}
	return updates
}
