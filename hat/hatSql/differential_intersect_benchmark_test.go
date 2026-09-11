package hatSql

import (
	"fmt"
	"testing"
)

type differentialIntersectBenchmarkEvent struct {
	left   bool
	update DifferentialRow
}

var differentialIntersectBenchmarkSink int

func differentialIntersectBenchmarkFixtures() ([]DifferentialRow, []DifferentialRow, []differentialIntersectBenchmarkEvent) {
	left := make([]DifferentialRow, 512)
	right := make([]DifferentialRow, 512)
	for index := range left {
		key := fmt.Sprintf("base-%04d", index)
		row := Row{"value": int64(index)}
		left[index] = DifferentialRow{Key: key, Time: uint64(index), Diff: 1, Row: row}
		right[index] = DifferentialRow{Key: key, Time: uint64(index), Diff: 1, Row: row}
	}
	events := make([]differentialIntersectBenchmarkEvent, 0, 256)
	for index := 0; index < 128; index++ {
		key := fmt.Sprintf("late-%04d", index)
		row := Row{"value": int64(index + 512)}
		events = append(events,
			differentialIntersectBenchmarkEvent{
				left:   true,
				update: DifferentialRow{Key: key, Time: uint64(index + 512), Diff: 1, Row: row},
			},
			differentialIntersectBenchmarkEvent{
				update: DifferentialRow{Key: key, Time: uint64(index + 512), Diff: 1, Row: row},
			},
		)
	}
	return left, right, events
}

func differentialIntersectBenchmarkSnapshot(left, right []DifferentialRow) []DifferentialRow {
	leftPresent := make(map[string]struct{}, len(left))
	rightPresent := make(map[string]struct{}, len(right))
	for _, update := range left {
		if update.Diff > 0 {
			leftPresent[update.Key] = struct{}{}
		}
	}
	for _, update := range right {
		if update.Diff > 0 {
			rightPresent[update.Key] = struct{}{}
		}
	}
	result := make([]DifferentialRow, 0, len(leftPresent))
	for _, update := range left {
		if _, exists := rightPresent[update.Key]; !exists {
			continue
		}
		if _, exists := leftPresent[update.Key]; !exists {
			continue
		}
		result = append(result, update)
		delete(leftPresent, update.Key)
	}
	return result
}

func BenchmarkDifferentialIntersect(b *testing.B) {
	left, right, events := differentialIntersectBenchmarkFixtures()
	b.Run("RebuildSnapshot", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			currentLeft := append([]DifferentialRow(nil), left...)
			currentRight := append([]DifferentialRow(nil), right...)
			for _, event := range events {
				if event.left {
					currentLeft = append(currentLeft, event.update)
				} else {
					currentRight = append(currentRight, event.update)
				}
				result := differentialIntersectBenchmarkSnapshot(currentLeft, currentRight)
				differentialIntersectBenchmarkSink += len(result)
			}
		}
	})
	b.Run("Incremental", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			operator := NewDifferentialIntersect()
			if _, err := operator.Apply(left, right); err != nil {
				b.Fatal(err)
			}
			for _, event := range events {
				var result []DifferentialRow
				var err error
				if event.left {
					result, err = operator.Apply([]DifferentialRow{event.update}, nil)
				} else {
					result, err = operator.Apply(nil, []DifferentialRow{event.update})
				}
				if err != nil {
					b.Fatal(err)
				}
				differentialIntersectBenchmarkSink += len(result)
			}
		}
	})
}
