//go:build t217

package hatDataStructure

import (
	"fmt"
	"runtime"
	"testing"
)

const t217BenchmarkRows = 4096

func newT217BenchmarkColumnarSpace(b *testing.B) *ColumnarSpace {
	b.Helper()
	space, err := NewColumnarSpace(ColumnarSpaceOptions{
		Name:     "benchmark",
		Capacity: t217BenchmarkRows,
		Columns: []ColumnarColumnSpec{
			{Name: "id", Kind: ColumnarInt64},
			{Name: "score", Kind: ColumnarFloat64},
			{Name: "region", Kind: ColumnarString},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < t217BenchmarkRows; index++ {
		if err := space.Append([]ColumnarValue{
			{Kind: ColumnarInt64, Valid: true, Int64: int64(index)},
			{Kind: ColumnarFloat64, Valid: true, Float64: float64(index) * 0.5},
			{Kind: ColumnarString, Valid: true, String: fmt.Sprintf("region-%02d", index&15)},
		}); err != nil {
			b.Fatal(err)
		}
	}
	return space
}

func newT217BenchmarkRowMaps() []map[string]interface{} {
	rows := make([]map[string]interface{}, t217BenchmarkRows)
	for index := range rows {
		rows[index] = map[string]interface{}{
			"id":     int64(index),
			"score":  float64(index) * 0.5,
			"region": fmt.Sprintf("region-%02d", index&15),
		}
	}
	return rows
}

func BenchmarkT217BuildColumnar(b *testing.B) {
	for range b.N {
		space := newT217BenchmarkColumnarSpace(b)
		if err := space.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT217BuildRowMaps(b *testing.B) {
	for range b.N {
		rows := newT217BenchmarkRowMaps()
		runtime.KeepAlive(rows)
	}
}

func BenchmarkT217ProjectColumnarInt64(b *testing.B) {
	space := newT217BenchmarkColumnarSpace(b)
	defer space.Close()
	column, found, err := space.Column("id")
	if err != nil || !found {
		b.Fatal(err)
	}
	b.ResetTimer()
	for range b.N {
		var sum int64
		for _, value := range column.Int64Values {
			sum += value
		}
		runtime.KeepAlive(sum)
	}
}

func BenchmarkT217ProjectRowMapsInt64(b *testing.B) {
	rows := newT217BenchmarkRowMaps()
	b.ResetTimer()
	for range b.N {
		var sum int64
		for _, row := range rows {
			sum += row["id"].(int64)
		}
		runtime.KeepAlive(sum)
	}
}

func BenchmarkT217RetainedBuffers(b *testing.B) {
	b.Run("columnar", func(b *testing.B) {
		space := newT217BenchmarkColumnarSpace(b)
		defer space.Close()
		b.ReportMetric(float64(space.MemoryBytes()), "retained_buffer_bytes")
		for range b.N {
			runtime.KeepAlive(space)
		}
	})
	b.Run("row_maps", func(b *testing.B) {
		rows := newT217BenchmarkRowMaps()
		b.ReportMetric(float64(t217RowMapPayloadBytes(rows)), "estimated_payload_bytes")
		for range b.N {
			runtime.KeepAlive(rows)
		}
	})
}

func t217RowMapPayloadBytes(rows []map[string]interface{}) int {
	var total int
	for _, row := range rows {
		total += len(row["region"].(string))
		total += 8 + 8
	}
	return total
}
