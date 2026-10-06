package hatReplication

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConflictEventLogRecordsRedactedBoundedStream(t *testing.T) {
	log, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 2})
	if err != nil {
		t.Fatalf("NewConflictEventLog() error = %v", err)
	}
	left := ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 2, NodeID: "node-b", Sequence: 1}
	first, err := log.Record("orders", []byte("secret-order-1"), left, right, ConflictEventDecisionRight)
	if err != nil {
		t.Fatalf("Record(first) error = %v", err)
	}
	second, err := log.Record("orders", []byte("secret-order-2"), left, right, ConflictEventDecisionLeft)
	if err != nil {
		t.Fatalf("Record(second) error = %v", err)
	}
	third, err := log.Record("orders", []byte("secret-order-3"), left, right, ConflictEventDecisionRejected)
	if err != nil {
		t.Fatalf("Record(third) error = %v", err)
	}
	if first.Sequence != 1 || second.Sequence != 2 || third.Sequence != 3 {
		t.Fatalf("sequences = %d, %d, %d, want 1, 2, 3", first.Sequence, second.Sequence, third.Sequence)
	}
	snapshot := log.Snapshot()
	if snapshot.Dropped != 1 || len(snapshot.Events) != 2 {
		t.Fatalf("snapshot = %+v, want one dropped and two retained", snapshot)
	}
	if snapshot.Events[0].Sequence != 2 || snapshot.Events[1].Sequence != 3 {
		t.Fatalf("retained sequences = %+v, want 2 and 3", snapshot.Events)
	}
	encoded, err := EncodeConflictEventSnapshot(snapshot)
	if err != nil {
		t.Fatalf("EncodeConflictEventSnapshot() error = %v", err)
	}
	if bytes.Contains(encoded, []byte("secret-order-3")) {
		t.Fatal("encoded event contains the raw conflict key")
	}
	if _, err := log.Since(0, 10); !errors.Is(err, ErrConflictEventGap) {
		t.Fatalf("Since(0) error = %v, want ErrConflictEventGap", err)
	}
	page, err := log.Since(1, 10)
	if err != nil {
		t.Fatalf("Since(1) error = %v", err)
	}
	if len(page.Events) != 2 || page.NextSequence != 3 || page.EarliestSequence != 2 || page.LatestSequence != 3 {
		t.Fatalf("page = %+v, want sequences 2..3", page)
	}
}

func TestConflictPolicyResolveAndRecord(t *testing.T) {
	log, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 8})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{Mode: ConflictPolicySourcePriority, SourcePriority: []string{"node-b", "node-a"}})
	if err != nil {
		t.Fatal(err)
	}
	left := ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 2, NodeID: "node-b", Sequence: 1}
	winner, err := registry.ResolveAndRecord(log, "orders", []byte("order-1"), left, right)
	if err != nil {
		t.Fatalf("ResolveAndRecord() error = %v", err)
	}
	if winner != right {
		t.Fatalf("winner = %+v, want right %+v", winner, right)
	}
	page, err := log.Since(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 1 || page.Events[0].Decision != ConflictEventDecisionRight {
		t.Fatalf("events = %+v, want one right decision", page.Events)
	}

	rejectLog, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 8})
	if err != nil {
		t.Fatal(err)
	}
	rejectRegistry, err := NewConflictPolicyRegistry(ConflictPolicy{Mode: ConflictPolicyReject})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rejectRegistry.ResolveAndRecord(rejectLog, "orders", []byte("order-2"), left, right); !errors.Is(err, ErrConflictRejected) {
		t.Fatalf("rejected ResolveAndRecord() error = %v, want ErrConflictRejected", err)
	}
	rejectPage, err := rejectLog.Since(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rejectPage.Events) != 1 || rejectPage.Events[0].Decision != ConflictEventDecisionRejected {
		t.Fatalf("rejected events = %+v, want one rejected decision", rejectPage.Events)
	}

	equalLog, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 8})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.ResolveAndRecord(equalLog, "orders", nil, left, left); err != nil {
		t.Fatalf("equal ResolveAndRecord() error = %v", err)
	}
	if page := equalLog.Snapshot(); len(page.Events) != 0 {
		t.Fatalf("equal versions recorded = %+v, want no event", page.Events)
	}
}

func TestConflictEventSnapshotRoundTripAndCorruption(t *testing.T) {
	log, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 4})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Record("orders", []byte("order-1"), ConflictVersion{Timestamp: 1, NodeID: "a"}, ConflictVersion{Timestamp: 2, NodeID: "b"}, ConflictEventDecisionRight); err != nil {
		t.Fatal(err)
	}
	original := log.Snapshot()
	encoded, err := EncodeConflictEventSnapshot(original)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeConflictEventSnapshot(encoded)
	if err != nil {
		t.Fatalf("DecodeConflictEventSnapshot() error = %v", err)
	}
	if len(decoded.Events) != 1 || decoded.Events[0] != original.Events[0] || decoded.NextSequence != original.NextSequence {
		t.Fatalf("decoded = %+v, want %+v", decoded, original)
	}
	encoded[len(encoded)-1] ^= 1
	if _, err := DecodeConflictEventSnapshot(encoded); !errors.Is(err, ErrConflictEventCorrupt) {
		t.Fatalf("corrupt decode error = %v, want ErrConflictEventCorrupt", err)
	}
}

func TestConflictEventFileStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "conflicts.hce")
	store, err := NewFileConflictEventStore(path)
	if err != nil {
		t.Fatal(err)
	}
	log, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 4, Store: store})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Record("orders", []byte("order-1"), ConflictVersion{Timestamp: 1, NodeID: "a"}, ConflictVersion{Timestamp: 2, NodeID: "b"}, ConflictEventDecisionRight); err != nil {
		t.Fatal(err)
	}
	restored, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 4, Store: store})
	if err != nil {
		t.Fatalf("restore error = %v", err)
	}
	if snapshot := restored.Snapshot(); len(snapshot.Events) != 1 || snapshot.Events[0].Sequence != 1 {
		t.Fatalf("restored snapshot = %+v", snapshot)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("file mode = %o, want 600", info.Mode().Perm())
	}
}

func TestConflictEventLogValidatesLimitsAndSaveRollback(t *testing.T) {
	if _, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: MaxConflictEventLogCapacity + 1}); !errors.Is(err, ErrConflictEventLogOptionsInvalid) {
		t.Fatalf("oversized capacity error = %v", err)
	}
	store := &conflictEventMemoryStore{saveErr: errors.New("save failed")}
	log, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 2, Store: store})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Record("orders", []byte("order"), ConflictVersion{Timestamp: 1, NodeID: "a"}, ConflictVersion{Timestamp: 2, NodeID: "b"}, ConflictEventDecisionRight); !errors.Is(err, store.saveErr) {
		t.Fatalf("save error = %v, want %v", err, store.saveErr)
	}
	if snapshot := log.Snapshot(); len(snapshot.Events) != 0 || snapshot.NextSequence != 0 {
		t.Fatalf("failed save left state = %+v", snapshot)
	}
	for _, space := range []string{"", "   ", strings.Repeat("x", MaxConflictEventSpaceBytes+1)} {
		if _, err := log.Record(space, nil, ConflictVersion{Timestamp: 1, NodeID: "a"}, ConflictVersion{Timestamp: 2, NodeID: "b"}, ConflictEventDecisionRight); !errors.Is(err, ErrConflictEventInvalid) {
			t.Fatalf("space %q error = %v, want ErrConflictEventInvalid", space, err)
		}
	}
	if _, err := log.Since(0, MaxConflictEventPageSize+1); !errors.Is(err, ErrConflictEventLimit) {
		t.Fatalf("oversized page error = %v, want ErrConflictEventLimit", err)
	}
}

type conflictEventMemoryStore struct {
	data    []byte
	saveErr error
}

func (store *conflictEventMemoryStore) Load() ([]byte, error) {
	return append([]byte(nil), store.data...), nil
}

func (store *conflictEventMemoryStore) Save(data []byte) error {
	if store.saveErr != nil {
		return store.saveErr
	}
	store.data = append(store.data[:0], data...)
	return nil
}
