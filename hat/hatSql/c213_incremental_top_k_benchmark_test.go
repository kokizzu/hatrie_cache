package hatSql

import (
	"fmt"
	"sort"
	"testing"
)

type c213TopKBenchmarkRow struct {
	key   string
	value int64
}

func BenchmarkMZ037TopKRebuildBaseline(b *testing.B) {
	const (
		rows = 10000
		k    = 20
	)

	values := make([]c213TopKBenchmarkRow, rows)
	for index := range values {
		values[index] = c213TopKBenchmarkRow{
			key:   fmt.Sprintf("row-%05d", index),
			value: int64(index),
		}
	}
	sort.Slice(values, func(left, right int) bool {
		if values[left].value != values[right].value {
			return values[left].value > values[right].value
		}
		return values[left].key < values[right].key
	})
	previous := make(map[string]int64, k)
	for index := 0; index < k; index++ {
		previous[values[index].key] = values[index].value
	}
	b.ReportAllocs()
	b.ResetTimer()
	var checksum int64
	for iteration := 0; iteration < b.N; iteration++ {
		index := (iteration*7919 + 17) % rows
		values[index].value = int64((iteration*104729 + 23) % (rows * 4))
		sort.Slice(values, func(left, right int) bool {
			if values[left].value != values[right].value {
				return values[left].value > values[right].value
			}
			return values[left].key < values[right].key
		})
		current := make(map[string]int64, k)
		for index := 0; index < k; index++ {
			current[values[index].key] = values[index].value
			checksum += values[index].value
		}
		changes := make([]DifferentialRow, 0, k*2)
		for key, value := range previous {
			currentValue, exists := current[key]
			if !exists || currentValue != value {
				changes = append(changes, DifferentialRow{Key: key, Diff: -1, Row: Row{"score": value}})
			}
		}
		for key, value := range current {
			previousValue, exists := previous[key]
			if !exists || previousValue != value {
				changes = append(changes, DifferentialRow{Key: key, Diff: 1, Row: Row{"score": value}})
			}
		}
		checksum += int64(len(changes))
		previous = current
	}
	b.StopTimer()
	b.ReportMetric(float64(checksum), "checksum")
}

func BenchmarkMZ037TopKIncremental(b *testing.B) {
	const (
		rows = 10000
		k    = 20
	)

	topK, err := NewIncrementalTopK(IncrementalTopKDefinition{
		K:          k,
		OrderKey:   c213TopKOrderKey,
		Descending: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	initial := make([]DifferentialRow, rows)
	keys := make([]string, rows)
	for index := range initial {
		keys[index] = fmt.Sprintf("row-%05d", index)
		initial[index] = DifferentialRow{
			Key:  keys[index],
			Diff: 1,
			Row:  Row{"score": int64(index)},
		}
	}
	if _, err := topK.Apply(initial); err != nil {
		b.Fatal(err)
	}
	updates := []DifferentialRow{
		{Diff: -1},
		{Diff: 1, Row: Row{"score": int64(0)}},
	}
	b.ReportAllocs()
	b.ResetTimer()
	var checksum int64
	for iteration := 0; iteration < b.N; iteration++ {
		index := (iteration*7919 + 17) % rows
		updates[0].Key = keys[index]
		updates[1].Key = keys[index]
		updates[1].Row["score"] = int64((iteration*104729 + 23) % (rows * 4))
		changes, err := topK.Apply(updates)
		if err != nil {
			b.Fatal(err)
		}
		checksum += int64(len(changes))
	}
	b.StopTimer()
	b.ReportMetric(float64(checksum), "change_rows")
}
