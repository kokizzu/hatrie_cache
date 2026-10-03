package hatReplication

import (
	"encoding/json"
	"testing"
)

var globalTimestampOracleSnapshotBenchmarkSink []byte
var globalTimestampOracleSnapshotBenchmarkDecoded GlobalTimestampOracleSnapshot

func BenchmarkGlobalTimestampOracleSnapshotJSON(b *testing.B) {
	oracle, err := benchmarkGlobalTimestampOracleSnapshot()
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		payload, err := json.Marshal(oracle.Snapshot())
		if err != nil {
			b.Fatal(err)
		}
		globalTimestampOracleSnapshotBenchmarkSink = payload
	}
}

func BenchmarkGlobalTimestampOracleSnapshotBinary(b *testing.B) {
	oracle, err := benchmarkGlobalTimestampOracleSnapshot()
	if err != nil {
		b.Fatal(err)
	}
	snapshot := oracle.Snapshot()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		payload, err := MarshalGlobalTimestampOracleSnapshot(snapshot)
		if err != nil {
			b.Fatal(err)
		}
		globalTimestampOracleSnapshotBenchmarkSink = payload
	}
}

func BenchmarkGlobalTimestampOracleSnapshotBinaryDecode(b *testing.B) {
	oracle, err := benchmarkGlobalTimestampOracleSnapshot()
	if err != nil {
		b.Fatal(err)
	}
	payload, err := MarshalGlobalTimestampOracleSnapshot(oracle.Snapshot())
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		snapshot, err := UnmarshalGlobalTimestampOracleSnapshot(payload)
		if err != nil {
			b.Fatal(err)
		}
		globalTimestampOracleSnapshotBenchmarkDecoded = snapshot
	}
}

func BenchmarkGlobalTimestampOracleSnapshotJSONDecode(b *testing.B) {
	oracle, err := benchmarkGlobalTimestampOracleSnapshot()
	if err != nil {
		b.Fatal(err)
	}
	payload, err := json.Marshal(oracle.Snapshot())
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		var snapshot GlobalTimestampOracleSnapshot
		if err := json.Unmarshal(payload, &snapshot); err != nil {
			b.Fatal(err)
		}
		globalTimestampOracleSnapshotBenchmarkDecoded = snapshot
	}
}

func BenchmarkGlobalTimestampOracleFileStoreSave(b *testing.B) {
	oracle, err := benchmarkGlobalTimestampOracleSnapshot()
	if err != nil {
		b.Fatal(err)
	}
	store, err := NewGlobalTimestampOracleFileStore(GlobalTimestampOracleFileStoreOptions{Path: b.TempDir() + "/state.bin"})
	if err != nil {
		b.Fatal(err)
	}
	snapshot := oracle.Snapshot()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := store.SaveSnapshot(snapshot); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGlobalTimestampOracleFileStoreLoad(b *testing.B) {
	oracle, err := benchmarkGlobalTimestampOracleSnapshot()
	if err != nil {
		b.Fatal(err)
	}
	store, err := NewGlobalTimestampOracleFileStore(GlobalTimestampOracleFileStoreOptions{Path: b.TempDir() + "/state.bin"})
	if err != nil {
		b.Fatal(err)
	}
	if err := store.SaveSnapshot(oracle.Snapshot()); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		snapshot, err := store.LoadSnapshot()
		if err != nil {
			b.Fatal(err)
		}
		globalTimestampOracleSnapshotBenchmarkDecoded = snapshot
	}
}

func benchmarkGlobalTimestampOracleSnapshot() (*GlobalTimestampOracle, error) {
	oracle, err := NewGlobalTimestampOracle(7, 0)
	if err != nil {
		return nil, err
	}
	for index := 0; index < 64; index++ {
		if _, err := oracle.Reserve(GlobalTimestampRequest{
			Term:      7,
			NodeID:    "node-" + benchmarkDecimal(index),
			NodeEpoch: 1,
			Sequence:  1,
			Count:     1024,
		}); err != nil {
			return nil, err
		}
	}
	return oracle, nil
}

func benchmarkDecimal(value int) string {
	if value == 0 {
		return "0"
	}
	var digits [20]byte
	position := len(digits)
	for value > 0 {
		position--
		digits[position] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[position:])
}
