package hatSql

import "testing"

var ch002PhysicalPartBenchmarkSink int

func BenchmarkCH002PhysicalPartScanBaseline(b *testing.B) {
	parts := newCH002PhysicalPartBenchmarkBatches()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		matched := 0
		for _, part := range parts {
			for _, value := range part.Columns["id"] {
				if value.(int64) >= 48_000 {
					matched++
				}
			}
		}
		ch002PhysicalPartBenchmarkSink = matched
	}
}

func BenchmarkCH002PhysicalPartScanPruned(b *testing.B) {
	parts := newCH002PhysicalPartBenchmarkSourceParts()
	predicates := []sqlColumnarNumericFilter{{field: "id", operator: ">=", value: 48_000}}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		matched := 0
		for _, part := range pruneSQLColumnarSourceParts(parts, predicates) {
			for _, value := range part.Batch.Columns["id"] {
				if value.(int64) >= 48_000 {
					matched++
				}
			}
		}
		ch002PhysicalPartBenchmarkSink = matched
	}
}

func BenchmarkCH002PhysicalPartSingleSelectionAndScan(b *testing.B) {
	parts := newCH002PhysicalPartBenchmarkSourceParts()
	predicates := []sqlColumnarNumericFilter{{field: "id", operator: ">=", value: 65_536 - 1024}}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		index, count := sqlColumnarSourcePartSelection(parts, predicates)
		if count != 1 {
			b.Fatalf("selected part count = %d, want 1", count)
		}
		matched := 0
		for _, value := range parts[index].Batch.Columns["id"] {
			if value.(int64) >= 48_000 {
				matched++
			}
		}
		ch002PhysicalPartBenchmarkSink = matched
	}
}

func newCH002PhysicalPartBenchmarkBatches() []ColumnarBatch {
	const partCount = 64
	const rowsPerPart = 1024
	parts := make([]ColumnarBatch, partCount)
	for partIndex := range parts {
		values := make([]interface{}, rowsPerPart)
		start := int64(partIndex * rowsPerPart)
		for row := range values {
			values[row] = start + int64(row)
		}
		parts[partIndex] = ColumnarBatch{Columns: map[string][]interface{}{"id": values}, Rows: rowsPerPart}
	}
	return parts
}

func newCH002PhysicalPartBenchmarkSourceParts() []ColumnarSourcePart {
	batches := newCH002PhysicalPartBenchmarkBatches()
	parts := make([]ColumnarSourcePart, len(batches))
	for index, batch := range batches {
		start := int64(index * len(batch.Columns["id"]))
		parts[index] = ColumnarSourcePart{
			Batch: batch,
			Segments: &ColumnarNumericSegments{
				RowsPerSegment:     batch.Rows,
				SparsePrimaryField: "id",
				Columns: map[string][]ColumnarNumericSegment{"id": {{
					Minimum: float64(start),
					Maximum: float64(start + int64(batch.Rows) - 1),
					Valid:   true,
				}}},
			},
		}
	}
	return parts
}
