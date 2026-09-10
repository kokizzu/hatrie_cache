package hatSql

import (
	"strconv"
	"testing"
)

const (
	incrementalDistinctFrameBenchmarkRows      = 1025
	incrementalDistinctFrameBenchmarkPreceding = 7
)

var incrementalDistinctFrameBenchmarkSink int64

func BenchmarkIncrementalDistinctFrameWindow(b *testing.B) {
	rows := benchmarkDistinctFrameRows()
	b.Run("full_scan", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		var sink int64
		for index := 0; index < b.N; index++ {
			values := benchmarkRecomputeDistinctFrame(rows, incrementalDistinctFrameBenchmarkPreceding)
			sink += values[len(values)-1]
		}
		incrementalDistinctFrameBenchmarkSink = sink
	})
	b.Run("incremental", func(b *testing.B) {
		b.ReportAllocs()
		var sink int64
		var window *IncrementalFrameWindow
		seed := rows[:len(rows)-1]
		tailRows := benchmarkDistinctFrameTailRows()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if index%len(tailRows) == 0 {
				b.StopTimer()
				var err error
				window, err = benchmarkNewDistinctFrameWindow()
				if err != nil {
					b.Fatal(err)
				}
				if _, err := window.Append(seed); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
			updates, err := window.Append(tailRows[index%len(tailRows) : index%len(tailRows)+1])
			if err != nil {
				b.Fatal(err)
			}
			sink += updates[0].Row["distinct_count"].(int64)
		}
		incrementalDistinctFrameBenchmarkSink = sink
	})
}
func benchmarkDistinctFrameRows() []Row {
	rows := make([]Row, incrementalDistinctFrameBenchmarkRows)
	for index := range rows {
		var value interface{}
		if index%23 != 0 {
			value = int64(index % 128)
		}
		rows[index] = Row{
			"id":        "row-" + strconv.Itoa(index),
			"partition": "partition-" + strconv.Itoa(index%16),
			"order":     int64(index / 16),
			"value":     value,
		}
	}
	return rows
}

func benchmarkDistinctFrameTailRows() []Row {
	rows := make([]Row, 1024)
	for index := range rows {
		rows[index] = Row{
			"id":        "tail-" + strconv.Itoa(index),
			"partition": "partition-0",
			"order":     int64(64 + index),
			"value":     int64(index % 128),
		}
	}
	return rows
}

func benchmarkRecomputeDistinctFrame(rows []Row, preceding int) []int64 {
	values := make([]int64, len(rows))
	for index, row := range rows {
		partition := row["partition"].(string)
		distinct := make(map[int64]struct{}, preceding+1)
		seen := 0
		for candidate := index; candidate >= 0 && seen <= preceding; candidate-- {
			if rows[candidate]["partition"].(string) != partition {
				continue
			}
			seen++
			value, ok := rows[candidate]["value"].(int64)
			if ok {
				distinct[value] = struct{}{}
			}
		}
		values[index] = int64(len(distinct))
	}
	return values
}

func benchmarkNewDistinctFrameWindow() (*IncrementalFrameWindow, error) {
	return NewIncrementalFrameWindow(IncrementalFrameWindowDefinition{
		Kind:           IncrementalWindowFrameCountDistinctInt64,
		OutputColumn:   "distinct_count",
		FramePreceding: incrementalDistinctFrameBenchmarkPreceding,
		PartitionKey:   func(row Row) (string, error) { return row["partition"].(string), nil },
		OrderKey:       func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:         func(row Row) (string, error) { return row["id"].(string), nil },
		ValueKey:       func(row Row) (interface{}, error) { return row["value"], nil },
	})
}
