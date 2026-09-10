package hatSql

import (
	"strconv"
	"testing"
)

var benchmarkIncrementalFrameAverageResult []DifferentialRow

func BenchmarkIncrementalAverageFrameWindow(b *testing.B) {
	seed := benchmarkAverageFrameRows(1024)
	allRows := append(append([]Row(nil), seed...), Row{
		"id":        "tail-0",
		"partition": "p0",
		"order":     int64(64),
		"value":     int64(64),
	})
	b.Run("full_scan", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkIncrementalFrameAverageResult = benchmarkRecomputeAverageFrame(allRows, 7)
		}
	})
	b.Run("incremental", func(b *testing.B) {
		window := benchmarkNewAverageFrameWindow(b)
		if _, err := window.Append(seed); err != nil {
			b.Fatal(err)
		}
		tailIDs := benchmarkAverageFrameTailIDs(b.N)
		tail := Row{"partition": "p0", "order": int64(64), "value": int64(64)}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tail["id"] = tailIDs[i]
			tail["order"] = int64(64 + i)
			updates, err := window.Append([]Row{tail})
			if err != nil {
				b.Fatal(err)
			}
			benchmarkIncrementalFrameAverageResult = updates
		}
	})
}

func benchmarkAverageFrameRows(count int) []Row {
	rows := make([]Row, count)
	for index := range rows {
		value := interface{}(int64(index % 31))
		if index%17 == 0 {
			value = nil
		}
		rows[index] = Row{
			"id":        "seed-" + strconv.Itoa(index),
			"partition": "p" + strconv.Itoa(index%16),
			"order":     int64(index / 16),
			"value":     value,
		}
	}
	return rows
}

func benchmarkAverageFrameTailIDs(count int) []string {
	ids := make([]string, count)
	for index := range ids {
		ids[index] = "tail-" + strconv.Itoa(index)
	}
	return ids
}

func benchmarkNewAverageFrameWindow(b *testing.B) *IncrementalFrameWindow {
	b.Helper()
	window, err := NewIncrementalFrameWindow(IncrementalFrameWindowDefinition{
		Kind:           IncrementalWindowFrameAvgInt64,
		OutputColumn:   "frame_value",
		FramePreceding: 7,
		PartitionKey:   func(row Row) (string, error) { return row["partition"].(string), nil },
		OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
		ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
	})
	if err != nil {
		b.Fatal(err)
	}
	return window
}

func benchmarkRecomputeAverageFrame(rows []Row, preceding int) []DifferentialRow {
	queues := make(map[string][]interface{}, 16)
	updates := make([]DifferentialRow, 0, len(rows))
	for _, row := range rows {
		partition := row["partition"].(string)
		queue := queues[partition]
		if len(queue) > preceding {
			queue = queue[1:]
		}
		queue = append(queue, row["value"])
		queues[partition] = queue
		var sum int64
		count := 0
		for _, item := range queue {
			if item == nil {
				continue
			}
			sum += item.(int64)
			count++
		}
		value := interface{}(nil)
		if count > 0 {
			value = float64(sum) / float64(count)
		}
		updates = append(updates, DifferentialRow{
			Key:  row["id"].(string),
			Diff: 1,
			Row:  incrementalFrameWindowOutput(row, "frame_value", value),
		})
	}
	return updates
}
