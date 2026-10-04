package hatReplication

import (
	"bytes"
	"errors"
	"reflect"
	"sync"
	"testing"
)

func conflictDecisionVersion(timestamp int64, node string, sequence uint64) ConflictVersion {
	return ConflictVersion{Timestamp: timestamp, NodeID: node, Sequence: sequence}
}

func TestConflictDecisionLogRecordsRedactedDecisionsAndTails(t *testing.T) {
	log, err := NewConflictDecisionLog(2)
	if err != nil {
		t.Fatalf("NewConflictDecisionLog() error = %v", err)
	}

	first, err := log.Record("orders", "customer/42", conflictDecisionVersion(1, "node-a", 1), conflictDecisionVersion(2, "node-b", 1))
	if err != nil {
		t.Fatalf("Record(first) error = %v", err)
	}
	if first.Sequence != 1 || first.Space != "orders" || first.Winner.NodeID != "node-b" || first.Loser.NodeID != "node-a" {
		t.Fatalf("first event = %#v", first)
	}
	if first.KeyDigest == [16]byte{} {
		t.Fatal("first event has an empty key digest")
	}

	if _, err := log.Record("orders", "customer/43", conflictDecisionVersion(3, "node-a", 2), conflictDecisionVersion(3, "node-c", 1)); err != nil {
		t.Fatalf("Record(second) error = %v", err)
	}
	if _, err := log.Record("orders", "customer/44", conflictDecisionVersion(4, "node-a", 3), conflictDecisionVersion(4, "node-d", 1)); err != nil {
		t.Fatalf("Record(third) error = %v", err)
	}

	tail, err := log.Tail(1, 10)
	if err != nil {
		t.Fatalf("Tail() error = %v", err)
	}
	if tail.OldestSequence != 2 || tail.LatestSequence != 3 || tail.Dropped != 1 || len(tail.Events) != 2 {
		t.Fatalf("tail metadata/events = %#v", tail)
	}
	if tail.Events[0].Sequence != 2 || tail.Events[1].Sequence != 3 {
		t.Fatalf("tail events = %#v", tail.Events)
	}
	if _, err := log.Tail(0, 10); err != nil {
		t.Fatalf("Tail(from zero) error = %v", err)
	}
	if _, err := log.Tail(1, 0); !errors.Is(err, ErrConflictDecisionLogInvalid) {
		t.Fatalf("Tail(zero limit) error = %v, want invalid", err)
	}
}

func TestConflictDecisionLogRejectsInvalidDecisions(t *testing.T) {
	var zero ConflictDecisionLog
	if _, err := zero.Record("s", "k", conflictDecisionVersion(1, "a", 1), conflictDecisionVersion(2, "b", 1)); !errors.Is(err, ErrConflictDecisionLogInvalid) {
		t.Fatalf("zero-value Record() error = %v, want invalid", err)
	}
	if _, err := zero.Tail(0, 1); !errors.Is(err, ErrConflictDecisionLogInvalid) {
		t.Fatalf("zero-value Tail() error = %v, want invalid", err)
	}
	if _, err := zero.Snapshot(); !errors.Is(err, ErrConflictDecisionLogInvalid) {
		t.Fatalf("zero-value Snapshot() error = %v, want invalid", err)
	}
	if _, err := NewConflictDecisionLog(0); !errors.Is(err, ErrConflictDecisionLogInvalid) {
		t.Fatalf("NewConflictDecisionLog(0) error = %v, want invalid", err)
	}
	log, err := NewConflictDecisionLog(4)
	if err != nil {
		t.Fatalf("NewConflictDecisionLog() error = %v", err)
	}
	validLeft := conflictDecisionVersion(1, "node-a", 1)
	validRight := conflictDecisionVersion(2, "node-b", 1)
	tests := []struct {
		name  string
		space string
		key   string
		left  ConflictVersion
		right ConflictVersion
		want  error
	}{
		{name: "space", space: "", key: "k", left: validLeft, right: validRight, want: ErrConflictDecisionLogInvalid},
		{name: "key", space: "s", key: "", left: validLeft, right: validRight, want: ErrConflictDecisionLogInvalid},
		{name: "version", space: "s", key: "k", left: ConflictVersion{}, right: validRight, want: ErrConflictVersionInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := log.Record(test.space, test.key, test.left, test.right); !errors.Is(err, test.want) {
				t.Fatalf("Record() error = %v, want %v", err, test.want)
			}
		})
	}
	if _, err := log.Record("s", "k", validLeft, validLeft); !errors.Is(err, ErrConflictDecisionLogNotConflict) {
		t.Fatalf("Record(equal) error = %v, want not-conflict", err)
	}
}

func TestConflictDecisionLogSnapshotRoundTripsAndDetectsCorruption(t *testing.T) {
	log, err := NewConflictDecisionLog(3)
	if err != nil {
		t.Fatalf("NewConflictDecisionLog() error = %v", err)
	}
	for index := 0; index < 4; index++ {
		if _, err := log.Record("orders", "customer/"+string(rune('0'+index)), conflictDecisionVersion(int64(index), "node-a", uint64(index+1)), conflictDecisionVersion(int64(index+1), "node-b", uint64(index+1))); err != nil {
			t.Fatalf("Record(%d) error = %v", index, err)
		}
	}
	snapshot, err := log.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	repeated, err := log.Snapshot()
	if err != nil {
		t.Fatalf("second Snapshot() error = %v", err)
	}
	if !bytes.Equal(snapshot, repeated) {
		t.Fatal("Snapshot() is not deterministic")
	}
	restored, err := RestoreConflictDecisionLog(snapshot)
	if err != nil {
		t.Fatalf("RestoreConflictDecisionLog() error = %v", err)
	}
	originalTail, err := log.Tail(2, 10)
	if err != nil {
		t.Fatalf("original Tail() error = %v", err)
	}
	restoredTail, err := restored.Tail(2, 10)
	if err != nil {
		t.Fatalf("restored Tail() error = %v", err)
	}
	if !reflect.DeepEqual(restoredTail, originalTail) {
		t.Fatalf("restored tail = %#v, want %#v", restoredTail, originalTail)
	}
	restoredTail.Events[0].Space = "changed"
	unchanged, err := restored.Tail(2, 10)
	if err != nil {
		t.Fatalf("second restored Tail() error = %v", err)
	}
	if unchanged.Events[0].Space == "changed" {
		t.Fatal("Tail() returned mutable log state")
	}

	corrupt := append([]byte(nil), snapshot...)
	corrupt[len(corrupt)-1]++
	if _, err := RestoreConflictDecisionLog(corrupt); !errors.Is(err, ErrConflictDecisionLogCorrupt) {
		t.Fatalf("corrupt restore error = %v, want corrupt", err)
	}
	badMagic := append([]byte(nil), snapshot...)
	badMagic[0] = 'X'
	if _, err := RestoreConflictDecisionLog(badMagic); !errors.Is(err, ErrConflictDecisionLogInvalid) {
		t.Fatalf("bad magic restore error = %v, want invalid", err)
	}
}

func TestConflictDecisionLogConcurrentRecord(t *testing.T) {
	log, err := NewConflictDecisionLog(128)
	if err != nil {
		t.Fatalf("NewConflictDecisionLog() error = %v", err)
	}
	const workers = 16
	var wait sync.WaitGroup
	wait.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer wait.Done()
			if _, err := log.Record("orders", "customer/"+string(rune('a'+worker)), conflictDecisionVersion(1, "node-a", uint64(worker+1)), conflictDecisionVersion(2, "node-b", uint64(worker+1))); err != nil {
				t.Errorf("Record() error = %v", err)
			}
		}(worker)
	}
	wait.Wait()
	tail, err := log.Tail(0, workers)
	if err != nil || len(tail.Events) != workers {
		t.Fatalf("Tail() = %#v, error = %v", tail, err)
	}
}

func FuzzRestoreConflictDecisionLogNeverPanics(f *testing.F) {
	log, err := NewConflictDecisionLog(2)
	if err != nil {
		f.Fatal(err)
	}
	if _, err := log.Record("orders", "customer/42", conflictDecisionVersion(1, "node-a", 1), conflictDecisionVersion(2, "node-b", 1)); err != nil {
		f.Fatal(err)
	}
	snapshot, err := log.Snapshot()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(snapshot)
	f.Add([]byte("HCD1"))
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	f.Fuzz(func(t *testing.T, encoded []byte) {
		_, _ = RestoreConflictDecisionLog(encoded)
	})
}

func BenchmarkConflictDecisionResolveBaseline(b *testing.B) {
	left := conflictDecisionVersion(1, "node-a", 1)
	right := conflictDecisionVersion(2, "node-b", 1)
	var winner ConflictVersion
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		var err error
		winner, err = ResolveConflictVersion(left, right)
		if err != nil {
			b.Fatal(err)
		}
	}
	_ = winner
}

func BenchmarkConflictDecisionLogRecord(b *testing.B) {
	log, err := NewConflictDecisionLog(1024)
	if err != nil {
		b.Fatal(err)
	}
	left := conflictDecisionVersion(1, "node-a", 1)
	right := conflictDecisionVersion(2, "node-b", 1)
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if _, err := log.Record("orders", "customer/42", left, right); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConflictDecisionLogSnapshot(b *testing.B) {
	log, err := NewConflictDecisionLog(128)
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 128; index++ {
		if _, err := log.Record("orders", "customer/42", conflictDecisionVersion(1, "node-a", uint64(index+1)), conflictDecisionVersion(2, "node-b", uint64(index+1))); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		snapshot, err := log.Snapshot()
		if err != nil {
			b.Fatal(err)
		}
		conflictDecisionLogBenchmarkSink = snapshot
	}
	b.ReportMetric(float64(len(conflictDecisionLogBenchmarkSink)), "snapshot-bytes")
}

var conflictDecisionLogBenchmarkSink []byte
