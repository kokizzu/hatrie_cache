package hatSql

import "testing"

func BenchmarkM250DirectTemporalJoin(b *testing.B) {
	join, err := NewDifferentialTemporalJoin(DifferentialTemporalJoinDefinition{
		MaxTimeDistance: 0,
		LeftKey:         func(row SQLRow) string { return row["join"].(string) },
		RightKey:        func(row SQLRow) string { return row["join"].(string) },
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		timestamp := uint64(index + 1)
		left := DifferentialRow{Key: "left", Time: timestamp, Diff: 1, Row: Row{"join": "a"}}
		right := DifferentialRow{Key: "right", Time: timestamp, Diff: 1, Row: Row{"join": "a"}}
		if _, err := join.ApplyLeft([]DifferentialRow{left}); err != nil {
			b.Fatal(err)
		}
		if _, err := join.ApplyRight([]DifferentialRow{right}); err != nil {
			b.Fatal(err)
		}
		if _, err := join.Compact(timestamp+1, timestamp+1); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM250AlignedTemporalJoin(b *testing.B) {
	aligned, err := NewDifferentialTemporalJoinAligned(DifferentialTemporalJoinDefinition{
		MaxTimeDistance: 0,
		LeftKey:         func(row SQLRow) string { return row["join"].(string) },
		RightKey:        func(row SQLRow) string { return row["join"].(string) },
	}, DifferentialTemporalJoinAlignmentOptions{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		timestamp := uint64(index + 1)
		left := DifferentialRow{Key: "left", Time: timestamp, Diff: 1, Row: Row{"join": "a"}}
		right := DifferentialRow{Key: "right", Time: timestamp, Diff: 1, Row: Row{"join": "a"}}
		if _, err := aligned.ApplyLeft(timestamp+1, []DifferentialRow{left}); err != nil {
			b.Fatal(err)
		}
		if _, err := aligned.ApplyRight(timestamp+1, []DifferentialRow{right}); err != nil {
			b.Fatal(err)
		}
	}
}
