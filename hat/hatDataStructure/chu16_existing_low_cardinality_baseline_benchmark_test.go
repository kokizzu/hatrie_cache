package hatDataStructure

import (
	"strconv"
	"testing"
)

func chu16BaselineRows(size, distinct int) []string {
	rows := make([]string, size)
	for index := range rows {
		rows[index] = "value-" + strconv.Itoa(index%distinct)
	}
	return rows
}

func BenchmarkCHU16BaselineDictionaryBuild256(b *testing.B) {
	const size = 10_000
	rows := chu16BaselineRows(size, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		builder := NewLowCardinalityStringBuilder(LowCardinalityStringBuilderOptions{
			InitialCapacity:    256,
			InitialRowCapacity: size,
		})
		for _, row := range rows {
			if _, err := builder.Append(row); err != nil {
				b.Fatal(err)
			}
		}
		if _, err := builder.Build(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCHU16BaselineDictionaryBuild4096(b *testing.B) {
	const size = 10_000
	rows := chu16BaselineRows(size, 4096)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		builder := NewLowCardinalityStringBuilder(LowCardinalityStringBuilderOptions{
			InitialCapacity:    4096,
			InitialRowCapacity: size,
		})
		for _, row := range rows {
			if _, err := builder.Append(row); err != nil {
				b.Fatal(err)
			}
		}
		if _, err := builder.Build(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCHU16BaselinePlainBuild10000(b *testing.B) {
	const size = 10_000
	rows := chu16BaselineRows(size, size)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		values := append([]string(nil), rows...)
		if len(values) != size {
			b.Fatal(len(values))
		}
	}
}
