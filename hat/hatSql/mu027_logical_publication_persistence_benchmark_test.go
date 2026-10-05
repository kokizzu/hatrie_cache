package hatSql

import (
	"testing"
)

var mu027PublicationPersistenceBenchmarkSink int

func newMU027PublicationPersistenceBenchmark(b *testing.B) *SQLPublication {
	b.Helper()
	publication, err := NewSQLPublication("orders", []string{"id", "state", "meta"}, SQLPublicationOptions{MaxHistoryBatches: 128})
	if err != nil {
		b.Fatalf("NewSQLPublication() error = %v", err)
	}
	for revision := uint64(1); revision <= 128; revision++ {
		if err := publication.Append(SQLPublicationBatch{
			Revision: revision,
			Frontier: revision * 10,
			Deltas: []SQLPublicationDelta{{
				Row: Row{
					"id":    int64(revision),
					"state": "open",
					"meta":  map[string]interface{}{"region": "sg", "active": true},
				},
				Diff: 1,
			}},
		}); err != nil {
			b.Fatalf("Append() error = %v", err)
		}
	}
	return publication
}

func BenchmarkMU027PublicationPersistenceMarshal(b *testing.B) {
	publication := newMU027PublicationPersistenceBenchmark(b)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded, err := publication.MarshalBinary()
		if err != nil {
			b.Fatalf("MarshalBinary() error = %v", err)
		}
		mu027PublicationPersistenceBenchmarkSink = len(encoded)
	}
}

func BenchmarkMU027PublicationPersistenceUnmarshal(b *testing.B) {
	publication := newMU027PublicationPersistenceBenchmark(b)
	encoded, err := publication.MarshalBinary()
	if err != nil {
		b.Fatalf("MarshalBinary() error = %v", err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		restored, err := UnmarshalSQLPublication(encoded)
		if err != nil {
			b.Fatalf("UnmarshalSQLPublication() error = %v", err)
		}
		mu027PublicationPersistenceBenchmarkSink = int(restored.Snapshot().LatestRevision)
	}
	b.ReportMetric(float64(len(encoded)), "snapshot_bytes")
}
