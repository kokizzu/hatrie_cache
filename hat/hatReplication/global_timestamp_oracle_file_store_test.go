package hatReplication

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestGlobalTimestampOracleFileStoreRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oracle", "state.bin")
	store, err := NewGlobalTimestampOracleFileStore(GlobalTimestampOracleFileStoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	oracle, err := NewGlobalTimestampOracle(9, 10)
	if err != nil {
		t.Fatal(err)
	}
	for index, nodeID := range []string{"node-b", "node-a"} {
		if _, err := oracle.Reserve(GlobalTimestampRequest{
			Term:      9,
			NodeID:    nodeID,
			NodeEpoch: 3,
			Sequence:  1,
			Observed:  int64(index),
			Count:     4,
		}); err != nil {
			t.Fatal(err)
		}
	}
	want := oracle.Snapshot()
	if err := store.SaveOracle(oracle); err != nil {
		t.Fatal(err)
	}
	got, err := store.LoadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshot mismatch: got %#v want %#v", got, want)
	}
	restored, err := store.LoadOracle()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(restored.Snapshot(), want) {
		t.Fatalf("restored oracle mismatch: got %#v want %#v", restored.Snapshot(), want)
	}
	firstPayload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSnapshot(want); err != nil {
		t.Fatal(err)
	}
	secondPayload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(secondPayload, firstPayload) {
		t.Fatalf("repeated snapshot bytes differ: first=%x second=%x", firstPayload, secondPayload)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("state mode = %o, want 600", info.Mode().Perm())
	}
	directoryInfo, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if directoryInfo.Mode().Perm() != 0o700 {
		t.Fatalf("state directory mode = %o, want 700", directoryInfo.Mode().Perm())
	}
}

func TestGlobalTimestampOracleFileStoreRejectsCorruptionAndSymlink(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.bin")
	store, err := NewGlobalTimestampOracleFileStore(GlobalTimestampOracleFileStoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	oracle, err := NewGlobalTimestampOracle(1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := oracle.Reserve(GlobalTimestampRequest{Term: 1, NodeID: "node-a", NodeEpoch: 1, Sequence: 1, Count: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSnapshot(oracle.Snapshot()); err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	payload[len(payload)/2] ^= 1
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadSnapshot(); !errors.Is(err, ErrGlobalTimestampOracleFileStoreCorrupt) {
		t.Fatalf("corrupt load error = %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(dir, "other"), path); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadSnapshot(); !errors.Is(err, ErrGlobalTimestampOracleFileStoreSymlink) {
		t.Fatalf("symlink load error = %v", err)
	}
	if err := store.SaveSnapshot(oracle.Snapshot()); !errors.Is(err, ErrGlobalTimestampOracleFileStoreSymlink) {
		t.Fatalf("symlink save error = %v", err)
	}
}

func TestGlobalTimestampOracleFileStoreBoundsInput(t *testing.T) {
	if _, err := NewGlobalTimestampOracleFileStore(GlobalTimestampOracleFileStoreOptions{}); !errors.Is(err, ErrGlobalTimestampOracleFileStoreInvalid) {
		t.Fatalf("empty options error = %v", err)
	}
	if _, err := NewGlobalTimestampOracleFileStore(GlobalTimestampOracleFileStoreOptions{Path: "state", MaxBytes: MaxGlobalTimestampOracleFileStoreBytes + 1}); !errors.Is(err, ErrGlobalTimestampOracleFileStoreInvalid) {
		t.Fatalf("oversized options error = %v", err)
	}
	path := filepath.Join(t.TempDir(), "state.bin")
	store, err := NewGlobalTimestampOracleFileStore(GlobalTimestampOracleFileStoreOptions{Path: path, MaxBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	oracle, err := NewGlobalTimestampOracle(1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := oracle.Reserve(GlobalTimestampRequest{Term: 1, NodeID: "node-a", NodeEpoch: 1, Sequence: 1, Count: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSnapshot(oracle.Snapshot()); !errors.Is(err, ErrGlobalTimestampOracleFileStoreTooLarge) {
		t.Fatalf("small store error = %v", err)
	}
}

func TestGlobalTimestampOracleSnapshotBinaryIsCompactAndEquivalent(t *testing.T) {
	oracle, err := benchmarkGlobalTimestampOracleSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	snapshot := oracle.Snapshot()
	jsonPayload, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	binaryPayload, err := MarshalGlobalTimestampOracleSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(binaryPayload) >= len(jsonPayload) {
		t.Fatalf("binary payload = %d bytes, JSON payload = %d bytes", len(binaryPayload), len(jsonPayload))
	}
	decoded, err := UnmarshalGlobalTimestampOracleSnapshot(binaryPayload)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, snapshot) {
		t.Fatalf("decoded snapshot mismatch: got %#v want %#v", decoded, snapshot)
	}
	t.Logf("json_bytes=%d binary_bytes=%d", len(jsonPayload), len(binaryPayload))
}
