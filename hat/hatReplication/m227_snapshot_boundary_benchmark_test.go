package hatReplication

import (
	"encoding/json"
	"testing"
)

func BenchmarkChangefeedSnapshotBoundaryCommitAndAdvance(b *testing.B) {
	boundary, err := NewChangefeedSnapshotBoundary("orders")
	if err != nil {
		b.Fatal(err)
	}
	if _, err := boundary.CommitSnapshotBoundary(100, 100); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := boundary.AdvanceLiveFrontier(uint64(index + 100)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkChangefeedSnapshotBoundaryMarshalBinary(b *testing.B) {
	snapshot := benchmarkChangefeedSnapshotBoundary()
	b.ReportAllocs()
	b.SetBytes(int64(len(mustMarshalChangefeedSnapshotBoundaryBinary(b, snapshot))))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := snapshot.MarshalBinary(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkChangefeedSnapshotBoundaryMarshalJSONControl(b *testing.B) {
	snapshot := benchmarkChangefeedSnapshotBoundary()
	encoded := mustMarshalChangefeedSnapshotBoundaryJSON(b, snapshot)
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := json.Marshal(snapshot); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkChangefeedSnapshotBoundaryUnmarshalBinary(b *testing.B) {
	snapshot := benchmarkChangefeedSnapshotBoundary()
	encoded := mustMarshalChangefeedSnapshotBoundaryBinary(b, snapshot)
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := UnmarshalChangefeedSnapshotBoundary(encoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkChangefeedSnapshotBoundaryUnmarshalJSONControl(b *testing.B) {
	snapshot := benchmarkChangefeedSnapshotBoundary()
	encoded := mustMarshalChangefeedSnapshotBoundaryJSON(b, snapshot)
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		var decoded ChangefeedSnapshotBoundarySnapshot
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}

func TestChangefeedSnapshotBoundaryBinaryIsSmallerThanJSON(t *testing.T) {
	snapshot := benchmarkChangefeedSnapshotBoundary()
	binarySnapshot := mustMarshalChangefeedSnapshotBoundaryBinary(t, snapshot)
	jsonSnapshot := mustMarshalChangefeedSnapshotBoundaryJSON(t, snapshot)
	if len(binarySnapshot) >= len(jsonSnapshot) {
		t.Fatalf("binary boundary should be smaller: binary=%d json=%d", len(binarySnapshot), len(jsonSnapshot))
	}
	t.Logf("boundary bytes: binary=%d json=%d ratio=%.2fx", len(binarySnapshot), len(jsonSnapshot), float64(len(jsonSnapshot))/float64(len(binarySnapshot)))
}

func benchmarkChangefeedSnapshotBoundary() ChangefeedSnapshotBoundarySnapshot {
	return ChangefeedSnapshotBoundarySnapshot{
		Source:            "orders",
		SnapshotOffset:    100,
		FirstLiveFrontier: 100,
		LiveFrontier:      150,
		Committed:         true,
	}
}

func mustMarshalChangefeedSnapshotBoundaryBinary(tb testing.TB, snapshot ChangefeedSnapshotBoundarySnapshot) []byte {
	tb.Helper()
	encoded, err := snapshot.MarshalBinary()
	if err != nil {
		tb.Fatal(err)
	}
	return encoded
}

func mustMarshalChangefeedSnapshotBoundaryJSON(tb testing.TB, snapshot ChangefeedSnapshotBoundarySnapshot) []byte {
	tb.Helper()
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		tb.Fatal(err)
	}
	return encoded
}
