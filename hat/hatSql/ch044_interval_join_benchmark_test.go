package hatSql

import "testing"

func BenchmarkCH044IntervalJoinBucketMaintenance(b *testing.B) {
	updates := make([]IncrementalIntervalJoinUpdate, 2048)
	for index := range updates {
		start := int64(index * 2)
		updates[index] = IncrementalIntervalJoinUpdate{
			Side: IncrementalIntervalJoinRight,
			Row: DifferentialRow{
				Key:  "right-" + benchmarkCH044Integer(index),
				Diff: 1,
				Row:  Row{"group": "hot", "start": start, "end": start + 1},
			},
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		join, err := NewIncrementalIntervalJoin(m029TestIntervalJoinDefinition())
		if err != nil {
			b.Fatal(err)
		}
		if _, err := join.Apply(updates); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkCH044Integer(value int) string {
	if value == 0 {
		return "0"
	}
	var digits [20]byte
	index := len(digits)
	for value > 0 {
		index--
		digits[index] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[index:])
}
