package hatReplication

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

var m033dGlobalTimestampSnapshotSink []byte
var m033dGlobalTimestampSnapshotDecodedSink GlobalTimestampOracleSnapshot

func m033dGlobalTimestampSnapshotFixture() GlobalTimestampOracleSnapshot {
	return GlobalTimestampOracleSnapshot{
		Term:    19,
		Current: 987654,
		Nodes: []GlobalTimestampNodeSnapshot{
			{
				NodeID:    "region-a-writer",
				NodeEpoch: 7,
				Sequence:  120,
				Observed:  987600,
				Grant: GlobalTimestampGrant{
					Term: 19, NodeID: "region-a-writer", NodeEpoch: 7,
					Sequence: 120, Start: 987601, End: 987620, Count: 20,
				},
			},
			{
				NodeID:    "region-b-writer",
				NodeEpoch: 4,
				Sequence:  81,
				Observed:  987500,
				Grant: GlobalTimestampGrant{
					Term: 19, NodeID: "region-b-writer", NodeEpoch: 4,
					Sequence: 81, Start: 987501, End: 987510, Count: 10,
				},
			},
		},
	}
}

func BenchmarkM033DGlobalTimestampSnapshotJSON(b *testing.B) {
	snapshot := m033dGlobalTimestampSnapshotFixture()
	b.ReportAllocs()
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(encoded)), "wire-bytes/op")
	b.ResetTimer()
	for range b.N {
		encoded, err := json.Marshal(snapshot)
		if err != nil {
			b.Fatal(err)
		}
		m033dGlobalTimestampSnapshotSink = encoded
	}
	b.ReportMetric(float64(len(encoded)), "wire-bytes/op")
}

func TestM033DGlobalTimestampSnapshotBinaryRoundTrip(t *testing.T) {
	snapshot := m033dGlobalTimestampSnapshotFixture()
	encoded, err := MarshalGlobalTimestampOracleSnapshotBinary(snapshot)
	if err != nil {
		t.Fatalf("MarshalGlobalTimestampOracleSnapshotBinary() error = %v", err)
	}
	jsonEncoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) >= len(jsonEncoded) {
		t.Fatalf("binary size = %d, JSON size = %d; want binary smaller", len(encoded), len(jsonEncoded))
	}
	t.Logf("snapshot wire sizes: binary=%d bytes, JSON=%d bytes", len(encoded), len(jsonEncoded))
	decoded, err := UnmarshalGlobalTimestampOracleSnapshotBinary(encoded)
	if err != nil {
		t.Fatalf("UnmarshalGlobalTimestampOracleSnapshotBinary() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, snapshot) {
		t.Fatalf("decoded snapshot = %#v, want %#v", decoded, snapshot)
	}
	unsorted := snapshot
	unsorted.Nodes = []GlobalTimestampNodeSnapshot{snapshot.Nodes[1], snapshot.Nodes[0]}
	canonical, err := MarshalGlobalTimestampOracleSnapshotBinary(unsorted)
	if err != nil {
		t.Fatalf("MarshalGlobalTimestampOracleSnapshotBinary(unsorted) error = %v", err)
	}
	canonicalDecoded, err := UnmarshalGlobalTimestampOracleSnapshotBinary(canonical)
	if err != nil {
		t.Fatalf("UnmarshalGlobalTimestampOracleSnapshotBinary(canonical) error = %v", err)
	}
	if !reflect.DeepEqual(canonicalDecoded, snapshot) {
		t.Fatalf("canonical decoded snapshot = %#v, want %#v", canonicalDecoded, snapshot)
	}
	duplicate := snapshot
	duplicate.Nodes = append(append([]GlobalTimestampNodeSnapshot(nil), snapshot.Nodes...), snapshot.Nodes[0])
	if _, err := MarshalGlobalTimestampOracleSnapshotBinary(duplicate); !errors.Is(err, ErrGlobalTimestampOracleSnapshotInvalid) {
		t.Fatalf("duplicate snapshot error = %v, want ErrGlobalTimestampOracleSnapshotInvalid", err)
	}
	encoded[7] ^= 1
	if _, err := UnmarshalGlobalTimestampOracleSnapshotBinary(encoded); !errors.Is(err, ErrGlobalTimestampOracleSnapshotInvalid) {
		t.Fatalf("corrupt snapshot error = %v, want ErrGlobalTimestampOracleSnapshotInvalid", err)
	}
}

func TestM033DGlobalTimestampSnapshotFileStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oracle", "snapshot.bin")
	store, err := NewGlobalTimestampOracleSnapshotFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := m033dGlobalTimestampSnapshotFixture()
	if err := store.Save(snapshot); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Fatalf("snapshot mode = %o, want 600", mode)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !reflect.DeepEqual(loaded, snapshot) {
		t.Fatalf("loaded snapshot = %#v, want %#v", loaded, snapshot)
	}
	beforeInvalidSave, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	invalid := snapshot
	invalid.Current = -1
	if err := store.Save(invalid); !errors.Is(err, ErrGlobalTimestampOracleSnapshotInvalid) {
		t.Fatalf("invalid Save() error = %v, want ErrGlobalTimestampOracleSnapshotInvalid", err)
	}
	afterInvalidSave, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(afterInvalidSave, beforeInvalidSave) {
		t.Fatal("invalid Save() changed the existing snapshot")
	}
	if err := os.WriteFile(path, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(); !errors.Is(err, ErrGlobalTimestampOracleSnapshotInvalid) {
		t.Fatalf("corrupt file error = %v, want ErrGlobalTimestampOracleSnapshotInvalid", err)
	}
}

func BenchmarkM033DGlobalTimestampSnapshotBinary(b *testing.B) {
	snapshot := m033dGlobalTimestampSnapshotFixture()
	b.ReportAllocs()
	encoded, err := MarshalGlobalTimestampOracleSnapshotBinary(snapshot)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(encoded)), "wire-bytes/op")
	b.ResetTimer()
	for range b.N {
		encoded, err := MarshalGlobalTimestampOracleSnapshotBinary(snapshot)
		if err != nil {
			b.Fatal(err)
		}
		m033dGlobalTimestampSnapshotSink = encoded
	}
	b.ReportMetric(float64(len(encoded)), "wire-bytes/op")
}

func BenchmarkM033DGlobalTimestampSnapshotBinaryDecode(b *testing.B) {
	encoded, err := MarshalGlobalTimestampOracleSnapshotBinary(m033dGlobalTimestampSnapshotFixture())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ReportMetric(float64(len(encoded)), "wire-bytes/op")
	b.ResetTimer()
	for range b.N {
		decoded, err := UnmarshalGlobalTimestampOracleSnapshotBinary(encoded)
		if err != nil {
			b.Fatal(err)
		}
		if decoded.Current == 0 {
			b.Fatal("decoded current timestamp is zero")
		}
	}
	b.ReportMetric(float64(len(encoded)), "wire-bytes/op")
}

func BenchmarkM033DGlobalTimestampSnapshotJSONDecode(b *testing.B) {
	encoded, err := json.Marshal(m033dGlobalTimestampSnapshotFixture())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ReportMetric(float64(len(encoded)), "wire-bytes/op")
	b.ResetTimer()
	for range b.N {
		var decoded GlobalTimestampOracleSnapshot
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			b.Fatal(err)
		}
		m033dGlobalTimestampSnapshotDecodedSink = decoded
	}
	b.ReportMetric(float64(len(encoded)), "wire-bytes/op")
}
