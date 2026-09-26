package hatSql

import "testing"

func BenchmarkDifferentialIntersectSameKeyBatch(b *testing.B) {
	operator := NewDifferentialIntersect()
	row := Row{"id": int64(1)}
	if _, err := operator.Apply([]DifferentialRow{{Key: "k", Diff: 1, Row: row}}, nil); err != nil {
		b.Fatal(err)
	}
	updates := []DifferentialRow{
		{Key: "k", Diff: 1, Row: row},
		{Key: "k", Diff: -1, Row: row},
	}

	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		changes, err := operator.Apply(updates, nil)
		if err != nil {
			b.Fatal(err)
		}
		differentialIntersectBenchmarkSink += len(changes)
	}
}
