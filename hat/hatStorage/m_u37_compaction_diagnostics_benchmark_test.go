//go:build mu37

package hatStorage

import (
	"testing"
	"time"
)

func BenchmarkMU37CompactionDiagnosticsRecord(b *testing.B) {
	diagnostics, err := NewCompactionDiagnostics(CompactionDiagnosticsOptions{MaxArrangements: 1, HistoryPerArrangement: 8})
	if err != nil {
		b.Fatal(err)
	}
	if err := diagnostics.Register("orders"); err != nil {
		b.Fatal(err)
	}
	observation := CompactionObservation{
		Arrangement:         "orders",
		LogicalBytes:        100,
		PhysicalBytes:       140,
		CompactionDebtBytes: 40,
		InputBytes:          160,
		OutputBytes:         120,
		Duration:            2 * time.Millisecond,
		Outcome:             CompactionSucceeded,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		observation.LogicalBytes = uint64(index)
		if err := diagnostics.Record(observation); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMU37CompactionDiagnosticsSnapshot(b *testing.B) {
	diagnostics, err := NewCompactionDiagnostics(CompactionDiagnosticsOptions{MaxArrangements: 64, HistoryPerArrangement: 8})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 64; index++ {
		arrangement := "arrangement-" + string(rune('a'+index))
		if err := diagnostics.Register(arrangement); err != nil {
			b.Fatal(err)
		}
		if err := diagnostics.Record(CompactionObservation{Arrangement: arrangement, Outcome: CompactionSucceeded}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if len(diagnostics.Snapshot()) != 64 {
			b.Fatal("snapshot lost arrangements")
		}
	}
}
