package hatSql

import (
	"strconv"
	"testing"
)

var (
	benchmarkIncrementalFrameCountResult []DifferentialRow
	benchmarkIncrementalFrameSumResult   []DifferentialRow
)

func BenchmarkIncrementalFrameWindow(b *testing.B) {
	seed := benchmarkIncrementalFrameRows(1024, false)
	allRows := append(append([]Row(nil), seed...), Row{
		"id":        "tail-0",
		"partition": "p0",
		"order":     int64(64),
	})
	b.Run("count_full_scan", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkIncrementalFrameCountResult = benchmarkRecomputeFrame(allRows, 7, false)
		}
	})
	b.Run("count_incremental", func(b *testing.B) {
		window := benchmarkNewIncrementalFrameWindow(b, IncrementalWindowFrameCount)
		if _, err := window.Append(seed); err != nil {
			b.Fatal(err)
		}
		tailIDs := benchmarkIncrementalFrameTailIDs(b.N)
		tail := Row{"partition": "p0", "order": int64(64)}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tail["id"] = tailIDs[i]
			tail["order"] = int64(64 + i)
			updates, err := window.Append([]Row{tail})
			if err != nil {
				b.Fatal(err)
			}
			benchmarkIncrementalFrameCountResult = updates
		}
	})

	sumSeed := benchmarkIncrementalFrameRows(1024, true)
	sumAllRows := append(append([]Row(nil), sumSeed...), Row{
		"id":        "tail-0",
		"partition": "p0",
		"order":     int64(64),
		"value":     int64(64),
	})
	b.Run("sum_full_scan", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkIncrementalFrameSumResult = benchmarkRecomputeFrame(sumAllRows, 7, true)
		}
	})
	b.Run("sum_incremental", func(b *testing.B) {
		window := benchmarkNewIncrementalFrameWindow(b, IncrementalWindowFrameSumInt64)
		if _, err := window.Append(sumSeed); err != nil {
			b.Fatal(err)
		}
		tailIDs := benchmarkIncrementalFrameTailIDs(b.N)
		tail := Row{"partition": "p0", "order": int64(64), "value": int64(64)}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tail["id"] = tailIDs[i]
			tail["order"] = int64(64 + i)
			tail["value"] = int64(64 + i)
			updates, err := window.Append([]Row{tail})
			if err != nil {
				b.Fatal(err)
			}
			benchmarkIncrementalFrameSumResult = updates
		}
	})
}

type benchmarkIncrementalFrameContribution struct {
	value int64
	valid bool
}

func benchmarkIncrementalFrameRows(count int, withValue bool) []Row {
	rows := make([]Row, count)
	for index := range rows {
		row := Row{
			"id":        "seed-" + strconv.Itoa(index),
			"partition": "p" + strconv.Itoa(index%16),
			"order":     int64(index / 16),
		}
		if withValue {
			row["value"] = int64(index % 31)
		}
		rows[index] = row
	}
	return rows
}

func benchmarkIncrementalFrameTailIDs(count int) []string {
	ids := make([]string, count)
	for index := range ids {
		ids[index] = "tail-" + strconv.Itoa(index)
	}
	return ids
}

func benchmarkNewIncrementalFrameWindow(b *testing.B, kind IncrementalFrameWindowKind) *IncrementalFrameWindow {
	b.Helper()
	window, err := NewIncrementalFrameWindow(IncrementalFrameWindowDefinition{
		Kind:           kind,
		OutputColumn:   "frame_value",
		FramePreceding: 7,
		OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
		PartitionKey:   func(row Row) (string, error) { return row["partition"].(string), nil },
		ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
	})
	if err != nil {
		b.Fatal(err)
	}
	return window
}

func benchmarkRecomputeFrame(rows []Row, preceding int, sum bool) []DifferentialRow {
	queues := make(map[string][]benchmarkIncrementalFrameContribution, 16)
	updates := make([]DifferentialRow, 0, len(rows))
	for _, row := range rows {
		partition := row["partition"].(string)
		contribution := benchmarkIncrementalFrameContribution{valid: true}
		if sum {
			value := row["value"]
			if value == nil {
				contribution.valid = false
			} else {
				contribution.value = value.(int64)
			}
		}
		queue := queues[partition]
		if len(queue) > preceding {
			queue = queue[1:]
		}
		queue = append(queue, contribution)
		queues[partition] = queue

		value := interface{}(int64(len(queue)))
		if sum {
			total := int64(0)
			valid := false
			for _, item := range queue {
				if item.valid {
					total += item.value
					valid = true
				}
			}
			if valid {
				value = total
			} else {
				value = nil
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
