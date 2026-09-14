package hatSql

import (
	"fmt"
	"testing"
)

func BenchmarkMZ039DistinctRebuildBaseline(b *testing.B) {
	const keys = 10000
	counts := make([]int64, keys)
	active := make([]bool, keys)
	keyNames := make([]string, keys)
	for index := range counts {
		counts[index] = 1
		active[index] = true
		keyNames[index] = fmt.Sprintf("row-%05d", index)
	}
	b.ReportAllocs()
	b.ResetTimer()
	var checksum int64
	for iteration := 0; iteration < b.N; iteration++ {
		index := (iteration*7919 + 17) % keys
		if counts[index] == 0 {
			counts[index] = 1
		} else {
			counts[index]--
		}
		changes := make([]DifferentialRow, 0, 1)
		for keyIndex, count := range counts {
			currentActive := count > 0
			if currentActive == active[keyIndex] {
				continue
			}
			active[keyIndex] = currentActive
			diff := int64(1)
			if !currentActive {
				diff = -1
			}
			changes = append(changes, DifferentialRow{
				Key:  keyNames[keyIndex],
				Diff: diff,
				Row:  Row{"value": int64(keyIndex)},
			})
		}
		checksum += int64(len(changes))
	}
	b.StopTimer()
	b.ReportMetric(float64(checksum), "change_rows")
}

func BenchmarkMZ039DistinctIncremental(b *testing.B) {
	const keys = 10000
	distinct := NewIncrementalDistinct()
	keyNames := make([]string, keys)
	counts := make([]int64, keys)
	initial := make([]DifferentialRow, keys)
	for index := range initial {
		keyNames[index] = fmt.Sprintf("row-%05d", index)
		counts[index] = 1
		initial[index] = DifferentialRow{
			Key:  keyNames[index],
			Diff: 1,
			Row:  Row{"value": int64(index)},
		}
	}
	if _, err := distinct.Apply(initial); err != nil {
		b.Fatal(err)
	}
	updates := make([]DifferentialRow, 1)
	b.ReportAllocs()
	b.ResetTimer()
	var checksum int64
	for iteration := 0; iteration < b.N; iteration++ {
		index := (iteration*7919 + 17) % keys
		updates[0] = DifferentialRow{Key: keyNames[index], Diff: -1}
		if counts[index] == 0 {
			counts[index] = 1
			updates[0].Diff = 1
			updates[0].Row = Row{"value": int64(index)}
		} else {
			counts[index]--
		}
		changes, err := distinct.Apply(updates)
		if err != nil {
			b.Fatal(err)
		}
		checksum += int64(len(changes))
	}
	b.StopTimer()
	b.ReportMetric(float64(checksum), "change_rows")
}
