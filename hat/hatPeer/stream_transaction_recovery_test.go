package hatPeer

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestStreamTransactionRecoveryPersistsAndRestores(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stream-recovery.bin")
	recovery, err := OpenStreamTransactionRecovery(StreamTransactionRecoveryOptions{
		Path:            path,
		MaxTransactions: 4,
		MaxOperations:   2,
		MaxPayloadBytes: 64,
	})
	if err != nil {
		t.Fatalf("OpenStreamTransactionRecovery() error = %v", err)
	}
	if err := recovery.Begin(11); err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	command := []byte("PUT")
	payload := []byte("value")
	if err := recovery.Record(11, 1, CompactPeerStreamCall, command, payload); err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	if err := recovery.Record(11, 1, CompactPeerStreamCall, command, payload); err != nil {
		t.Fatalf("idempotent Record() error = %v", err)
	}
	snapshot, ok := recovery.Lookup(11)
	if !ok || snapshot.State != StreamTransactionPending || len(snapshot.Operations) != 1 {
		t.Fatalf("Lookup() = %#v, %t", snapshot, ok)
	}
	buffered := StreamTransactionSnapshot{Operations: make([]StreamTransactionOperation, 1)}
	if !recovery.LookupInto(11, &buffered) || buffered.State != StreamTransactionPending || len(buffered.Operations) != 1 {
		t.Fatalf("LookupInto() = %#v", buffered)
	}
	buffered.Operations[0].CommandHash[0]++
	unchanged, _ := recovery.Lookup(11)
	if unchanged.Operations[0].CommandHash[0] == buffered.Operations[0].CommandHash[0] {
		t.Fatal("LookupInto() exposed internal operation storage")
	}
	if err := recovery.Commit(11); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if permission := info.Mode().Perm(); permission != 0o600 {
		t.Fatalf("snapshot permissions = %04o, want 0600", permission)
	}

	restored, err := OpenStreamTransactionRecovery(StreamTransactionRecoveryOptions{Path: path})
	if err != nil {
		t.Fatalf("restore OpenStreamTransactionRecovery() error = %v", err)
	}
	snapshot, ok = restored.Lookup(11)
	if !ok || snapshot.State != StreamTransactionCommitted || len(snapshot.Operations) != 1 {
		t.Fatalf("restored Lookup() = %#v, %t", snapshot, ok)
	}
	if err := restored.Forget(11); err != nil {
		t.Fatalf("Forget() error = %v", err)
	}
	if _, ok := restored.Lookup(11); ok {
		t.Fatal("Lookup() found forgotten transaction")
	}

	pendingPath := filepath.Join(t.TempDir(), "pending-recovery.bin")
	pending, err := OpenStreamTransactionRecovery(StreamTransactionRecoveryOptions{Path: pendingPath})
	if err != nil {
		t.Fatalf("pending OpenStreamTransactionRecovery() error = %v", err)
	}
	if err := pending.Begin(12); err != nil {
		t.Fatalf("pending Begin() error = %v", err)
	}
	if err := pending.Record(12, 1, CompactPeerStreamCall, []byte("SET"), []byte("pending")); err != nil {
		t.Fatalf("pending Record() error = %v", err)
	}
	if err := pending.Sync(); err != nil {
		t.Fatalf("pending Sync() error = %v", err)
	}
	restoredPending, err := OpenStreamTransactionRecovery(StreamTransactionRecoveryOptions{Path: pendingPath})
	if err != nil {
		t.Fatalf("restored pending OpenStreamTransactionRecovery() error = %v", err)
	}
	pendingSnapshot, ok := restoredPending.Lookup(12)
	if !ok || pendingSnapshot.State != StreamTransactionPending || len(pendingSnapshot.Operations) != 1 {
		t.Fatalf("restored pending Lookup() = %#v, %t", pendingSnapshot, ok)
	}
}

func TestStreamTransactionRecoveryBoundsAndCorruption(t *testing.T) {
	if _, err := NewStreamTransactionRecovery(StreamTransactionRecoveryOptions{SyncMode: 99}); !errors.Is(err, ErrStreamTransactionRecoverySyncModeInvalid) {
		t.Fatalf("invalid sync mode error = %v", err)
	}
	recovery, err := NewStreamTransactionRecovery(StreamTransactionRecoveryOptions{
		MaxTransactions: 1,
		MaxOperations:   1,
		MaxPayloadBytes: 3,
	})
	if err != nil {
		t.Fatalf("NewStreamTransactionRecovery() error = %v", err)
	}
	if err := recovery.Begin(1); err != nil {
		t.Fatalf("Begin(1) error = %v", err)
	}
	if err := recovery.Record(1, 1, CompactPeerStreamCall, []byte("SET"), []byte("long")); !errors.Is(err, ErrStreamTransactionPayloadTooLarge) {
		t.Fatalf("oversized Record() error = %v, want %v", err, ErrStreamTransactionPayloadTooLarge)
	}
	if err := recovery.Record(1, 1, CompactPeerStreamCall, []byte("SET"), []byte("ok")); err != nil {
		t.Fatalf("Record(1) error = %v", err)
	}
	if err := recovery.Record(1, 1, CompactPeerStreamCall, []byte("DEL"), []byte("ok")); !errors.Is(err, ErrStreamTransactionOperationConflict) {
		t.Fatalf("conflicting Record() error = %v, want %v", err, ErrStreamTransactionOperationConflict)
	}
	if err := recovery.Record(1, 2, CompactPeerStreamCall, []byte("SET"), []byte("ok")); !errors.Is(err, ErrStreamTransactionOperationLimit) {
		t.Fatalf("second Record() error = %v, want %v", err, ErrStreamTransactionOperationLimit)
	}
	if err := recovery.Begin(2); !errors.Is(err, ErrStreamTransactionCapacity) {
		t.Fatalf("Begin(2) error = %v, want %v", err, ErrStreamTransactionCapacity)
	}

	path := filepath.Join(t.TempDir(), "corrupt.bin")
	if err := os.WriteFile(path, []byte("not-a-recovery-snapshot"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if _, err := OpenStreamTransactionRecovery(StreamTransactionRecoveryOptions{Path: path}); !errors.Is(err, ErrStreamTransactionRecoveryCorrupt) {
		t.Fatalf("corrupt restore error = %v, want %v", err, ErrStreamTransactionRecoveryCorrupt)
	}
}
