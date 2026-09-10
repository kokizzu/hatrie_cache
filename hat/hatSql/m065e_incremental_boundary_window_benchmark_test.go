package hatSql

import (
	"strconv"
	"testing"
)

var (
	benchmarkIncrementalBoundaryFirstResult []DifferentialRow
	benchmarkIncrementalBoundaryLastResult  []DifferentialRow
)

func BenchmarkIncrementalBoundaryWindow(b *testing.B) {
	seed := benchmarkIncrementalBoundaryRows(1024)
	allRows := append(append([]Row(nil), seed...), Row{
		"id":        "tail-0",
		"partition": "p0",
		"order":     int64(64),
		"value":     "tail",
	})
	b.Run("first_full_scan", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkIncrementalBoundaryFirstResult = benchmarkRecomputeBoundaryWindow(allRows, true)
		}
	})
	b.Run("first_incremental", func(b *testing.B) {
		window := benchmarkNewIncrementalBoundaryWindow(b, IncrementalWindowFirstValue)
		if _, err := window.Append(seed); err != nil {
			b.Fatal(err)
		}
		tailIDs := benchmarkIncrementalBoundaryTailIDs(b.N)
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
			benchmarkIncrementalBoundaryFirstResult = updates
		}
	})
	b.Run("last_full_scan", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkIncrementalBoundaryLastResult = benchmarkRecomputeBoundaryWindow(allRows, false)
		}
	})
	b.Run("last_incremental", func(b *testing.B) {
		window := benchmarkNewIncrementalBoundaryWindow(b, IncrementalWindowLastValue)
		if _, err := window.Append(seed); err != nil {
			b.Fatal(err)
		}
		tailIDs := benchmarkIncrementalBoundaryTailIDs(b.N)
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
			benchmarkIncrementalBoundaryLastResult = updates
		}
	})
}

func benchmarkIncrementalBoundaryRows(count int) []Row {
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

func benchmarkIncrementalBoundaryTailIDs(count int) []string {
	ids := make([]string, count)
	for index := range ids {
		ids[index] = "tail-" + strconv.Itoa(index)
	}
	return ids
}

func benchmarkNewIncrementalBoundaryWindow(b *testing.B, kind IncrementalBoundaryWindowKind) *IncrementalBoundaryWindow {
	b.Helper()
	window, err := NewIncrementalBoundaryWindow(IncrementalBoundaryWindowDefinition{
		Kind:         kind,
		OutputColumn: "boundary_value",
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

func benchmarkRecomputeBoundaryWindow(rows []Row, first bool) []DifferentialRow {
	firstValues := make(map[string]interface{}, 16)
	updates := make([]DifferentialRow, 0, len(rows))
	for _, row := range rows {
		partition := row["partition"].(string)
		value := row["value"]
		if first {
			if _, exists := firstValues[partition]; !exists {
				firstValues[partition] = value
			}
			value = firstValues[partition]
		}
		updates = append(updates, DifferentialRow{
			Key:  row["id"].(string),
			Diff: 1,
			Row:  incrementalFrameWindowOutput(row, "boundary_value", value),
		})
	}
	return updates
}
