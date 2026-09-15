package hatDataStructure

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func BenchmarkLowCardinalityStringBuild(b *testing.B) {
	values := benchmarkLowCardinalityValues(100_000, 64)
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		builder := NewLowCardinalityStringBuilder(LowCardinalityStringBuilderOptions{InitialCapacity: 64, InitialRowCapacity: len(values)})
		for _, value := range values {
			if _, err := builder.Append(value); err != nil {
				b.Fatal(err)
			}
		}
		column, err := builder.Build()
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(column.MemoryBytes()), "retained-bytes")
	}
}

func BenchmarkPlainStringColumnBuild(b *testing.B) {
	values := benchmarkLowCardinalityValues(100_000, 64)
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		column := make([]string, len(values))
		copy(column, values)
		b.ReportMetric(float64(plainStringColumnMemoryBytes(column)), "retained-bytes")
	}
}

func BenchmarkLowCardinalityStringGroupByCodes(b *testing.B) {
	values := benchmarkLowCardinalityValues(100_000, 64)
	column := benchmarkLowCardinalityColumn(b, values)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		counts := make(map[uint32]int, column.Cardinality())
		for row := 0; row < column.Len(); row++ {
			code, valid := column.CodeAt(row)
			if valid {
				counts[code]++
			}
		}
		if len(counts) != column.Cardinality() {
			b.Fatalf("group count = %d, want %d", len(counts), column.Cardinality())
		}
	}
}

func BenchmarkPlainStringGroupByValues(b *testing.B) {
	values := benchmarkLowCardinalityValues(100_000, 64)
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		counts := make(map[string]int, 64)
		for _, value := range values {
			counts[value]++
		}
		if len(counts) != 64 {
			b.Fatalf("group count = %d, want 64", len(counts))
		}
	}
}

func BenchmarkLowCardinalityStringGroupByDenseCounts(b *testing.B) {
	values := benchmarkLowCardinalityValues(100_000, 64)
	column := benchmarkLowCardinalityColumn(b, values)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		counts := column.CountCodes()
		if len(counts) != column.Cardinality() || counts[0] == 0 {
			b.Fatalf("dense group counts = %#v", counts)
		}
	}
}

func BenchmarkLowCardinalityStringMarshal(b *testing.B) {
	values := benchmarkLowCardinalityValues(100_000, 64)
	column := benchmarkLowCardinalityColumn(b, values)
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		encoded, err := column.MarshalBinary()
		if err != nil {
			b.Fatal(err)
		}
		b.SetBytes(int64(len(encoded)))
		b.ReportMetric(float64(len(encoded)), "wire-bytes")
	}
}

func BenchmarkPlainStringMarshal(b *testing.B) {
	values := benchmarkLowCardinalityValues(100_000, 64)
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		encoded := marshalPlainBenchmarkStrings(values)
		if len(encoded) == 0 {
			b.Fatal("plain encoding is empty")
		}
		b.SetBytes(int64(len(encoded)))
		b.ReportMetric(float64(len(encoded)), "wire-bytes")
	}
}

func BenchmarkLowCardinalityStringHighCardinalityBuild(b *testing.B) {
	values := benchmarkLowCardinalityValues(20_000, 20_000)
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		builder := NewLowCardinalityStringBuilder(LowCardinalityStringBuilderOptions{InitialCapacity: 20_000, InitialRowCapacity: len(values), MaxDistinctValues: -1})
		for _, value := range values {
			if _, err := builder.Append(value); err != nil {
				b.Fatal(err)
			}
		}
		column, err := builder.Build()
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(column.MemoryBytes()), "retained-bytes")
	}
}

func BenchmarkPlainStringHighCardinalityBuild(b *testing.B) {
	values := benchmarkLowCardinalityValues(20_000, 20_000)
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		column := make([]string, len(values))
		copy(column, values)
		b.ReportMetric(float64(plainStringColumnMemoryBytes(column)), "retained-bytes")
	}
}

func benchmarkLowCardinalityColumn(b *testing.B, values []string) LowCardinalityStringColumn {
	b.Helper()
	builder := NewLowCardinalityStringBuilder(LowCardinalityStringBuilderOptions{InitialCapacity: 64, InitialRowCapacity: len(values)})
	for _, value := range values {
		if _, err := builder.Append(value); err != nil {
			b.Fatal(err)
		}
	}
	column, err := builder.Build()
	if err != nil {
		b.Fatal(err)
	}
	return column
}

func benchmarkLowCardinalityValues(rows, distinct int) []string {
	values := make([]string, rows)
	for row := range values {
		value := fmt.Sprintf("region-%05d", row%distinct)
		values[row] = string([]byte(value))
	}
	return values
}

func plainStringColumnMemoryBytes(values []string) int {
	bytes := len(values) * 16
	for _, value := range values {
		bytes += len(value)
	}
	return bytes
}

func marshalPlainBenchmarkStrings(values []string) []byte {
	encoded := make([]byte, 0, plainStringColumnMemoryBytes(values))
	for _, value := range values {
		var buffer [binary.MaxVarintLen64]byte
		length := binary.PutUvarint(buffer[:], uint64(len(value)))
		encoded = append(encoded, buffer[:length]...)
		encoded = append(encoded, value...)
	}
	return encoded
}
