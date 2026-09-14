package hatSql

import (
	"fmt"
	"sort"
	"testing"
)

type m040PercentileBenchmarkRow struct {
	key   string
	value int64
}

func BenchmarkMZ040PercentileRebuildBaseline(b *testing.B) {
	const rows = 10000
	values := make([]m040PercentileBenchmarkRow, rows)
	for index := range values {
		values[index] = m040PercentileBenchmarkRow{
			key:   fmt.Sprintf("row-%05d", index),
			value: int64(index),
		}
	}
	sort.Slice(values, func(left, right int) bool {
		if values[left].value != values[right].value {
			return values[left].value < values[right].value
		}
		return values[left].key < values[right].key
	})
	b.ReportAllocs()
	b.ResetTimer()
	var checksum int64
	for iteration := 0; iteration < b.N; iteration++ {
		index := (iteration*7919 + 17) % rows
		values[index].value = int64((iteration*104729 + 23) % (rows * 4))
		sort.Slice(values, func(left, right int) bool {
			if values[left].value != values[right].value {
				return values[left].value < values[right].value
			}
			return values[left].key < values[right].key
		})
		selected := values[9499]
		result := DifferentialRow{Key: selected.key, Diff: 1, Row: Row{"value": selected.value}}
		checksum += result.Row["value"].(int64)
	}
	b.StopTimer()
	b.ReportMetric(float64(checksum), "checksum")
}

func BenchmarkMZ040PercentileIncremental(b *testing.B) {
	const rows = 10000
	seed := make([]DifferentialRow, rows)
	for index := range seed {
		seed[index] = DifferentialRow{
			Key:  fmt.Sprintf("row-%05d", index),
			Diff: 1,
			Row:  Row{"value": int64(index)},
		}
	}
	percentile, err := NewIncrementalPercentile(IncrementalPercentileDefinition{
		OrderKey: m040PercentileOrderKey,
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := percentile.Apply(seed); err != nil {
		b.Fatal(err)
	}
	updates := []DifferentialRow{
		{Key: "row-00000", Diff: -1},
		{Key: "row-00000", Diff: 1, Row: Row{"value": int64(0)}},
	}
	b.ReportAllocs()
	b.ResetTimer()
	var checksum int64
	for iteration := 0; iteration < b.N; iteration++ {
		updates[1].Row["value"] = int64((iteration*104729 + 23) % (rows * 4))
		if err := percentile.Apply(updates); err != nil {
			b.Fatal(err)
		}
		selected, ok, err := percentile.Percentile(.95)
		if err != nil || !ok {
			b.Fatalf("Percentile() = %#v, %v, %v", selected, ok, err)
		}
		checksum += selected.Row["value"].(int64)
	}
	b.StopTimer()
	b.ReportMetric(float64(checksum), "checksum")
}
