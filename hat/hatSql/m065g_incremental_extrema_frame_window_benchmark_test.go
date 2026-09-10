package hatSql

import (
	"strconv"
	"testing"
)

var (
	benchmarkIncrementalFrameMinResult []DifferentialRow
	benchmarkIncrementalFrameMaxResult []DifferentialRow
)

func BenchmarkIncrementalExtremaFrameWindow(b *testing.B) {
	seed := benchmarkIncrementalExtremaFrameRows(1024)
	allRows := append(append([]Row(nil), seed...), Row{
		"id":        "tail-0",
		"partition": "p0",
		"order":     int64(64),
		"value":     int64(64),
	})
	b.Run("min_full_scan", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkIncrementalFrameMinResult = benchmarkRecomputeExtremaFrame(allRows, 7, true)
		}
	})
	b.Run("min_incremental", func(b *testing.B) {
		window := benchmarkNewIncrementalExtremaFrameWindow(b, IncrementalWindowFrameMinInt64)
		if _, err := window.Append(seed); err != nil {
			b.Fatal(err)
		}
		tailIDs := benchmarkIncrementalExtremaFrameTailIDs(b.N)
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
			benchmarkIncrementalFrameMinResult = updates
		}
	})
	b.Run("max_full_scan", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkIncrementalFrameMaxResult = benchmarkRecomputeExtremaFrame(allRows, 7, false)
		}
	})
	b.Run("max_incremental", func(b *testing.B) {
		window := benchmarkNewIncrementalExtremaFrameWindow(b, IncrementalWindowFrameMaxInt64)
		if _, err := window.Append(seed); err != nil {
			b.Fatal(err)
		}
		tailIDs := benchmarkIncrementalExtremaFrameTailIDs(b.N)
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
			benchmarkIncrementalFrameMaxResult = updates
		}
	})
}

func benchmarkIncrementalExtremaFrameRows(count int) []Row {
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

func benchmarkIncrementalExtremaFrameTailIDs(count int) []string {
	ids := make([]string, count)
	for index := range ids {
		ids[index] = "tail-" + strconv.Itoa(index)
	}
	return ids
}

func benchmarkNewIncrementalExtremaFrameWindow(b *testing.B, kind IncrementalFrameWindowKind) *IncrementalFrameWindow {
	b.Helper()
	window, err := NewIncrementalFrameWindow(IncrementalFrameWindowDefinition{
		Kind:           kind,
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

func benchmarkRecomputeExtremaFrame(rows []Row, preceding int, min bool) []DifferentialRow {
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
		value := interface{}(nil)
		for _, item := range queue {
			if item == nil {
				continue
			}
			if value == nil {
				value = item
				continue
			}
			itemValue := item.(int64)
			current := value.(int64)
			if min && itemValue < current {
				value = item
			}
			if !min && itemValue > current {
				value = item
			}
		}
		updates = append(updates, DifferentialRow{
			Key:  row["id"].(string),
			Diff: 1,
			Row:  incrementalFrameWindowOutput(row, "frame_value", value),
		})
	}
	return updates
}
