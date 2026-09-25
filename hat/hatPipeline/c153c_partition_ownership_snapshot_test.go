package hatPipeline

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"reflect"
	"testing"
)

func TestC153cQueuePartitionOwnershipSnapshotRoundTripAndAtomicRestore(t *testing.T) {
	ownership, err := NewQueuePartitionOwnership(QueuePartitionOwnershipOptions{
		PartitionCount: 3,
		InitialOwners:  []string{"node-a", "node-a", "node-b"},
	})
	if err != nil {
		t.Fatalf("NewQueuePartitionOwnership() error = %v", err)
	}
	migration, err := ownership.BeginMigration(1, "node-b", 42)
	if err != nil {
		t.Fatalf("BeginMigration() error = %v", err)
	}
	if _, err := ownership.AcknowledgeCatchUp(migration, 42); err != nil {
		t.Fatalf("AcknowledgeCatchUp() error = %v", err)
	}

	payload, err := ownership.MarshalSnapshot()
	if err != nil {
		t.Fatalf("MarshalSnapshot() error = %v", err)
	}
	if !bytes.HasPrefix(payload, []byte("QPO1")) {
		t.Fatalf("snapshot magic = %q, want QPO1", payload[:minC153c(len(payload), 4)])
	}

	restored, err := NewQueuePartitionOwnership(QueuePartitionOwnershipOptions{PartitionCount: 1})
	if err != nil {
		t.Fatalf("restore target constructor error = %v", err)
	}
	if err := restored.RestoreSnapshot(payload); err != nil {
		t.Fatalf("RestoreSnapshot() error = %v", err)
	}
	if got, want := restored.Assignments(), ownership.Assignments(); !reflect.DeepEqual(got, want) {
		t.Fatalf("restored assignments = %+v, want %+v", got, want)
	}

	before := restored.Assignments()
	corrupt := append([]byte(nil), payload...)
	corrupt[len(corrupt)-1] ^= 0xff
	if err := restored.RestoreSnapshot(corrupt); !errors.Is(err, ErrQueuePartitionOwnershipSnapshotChecksum) {
		t.Fatalf("corrupt snapshot error = %v, want checksum error", err)
	}
	if got := restored.Assignments(); !reflect.DeepEqual(got, before) {
		t.Fatalf("state changed after corrupt restore = %+v, want %+v", got, before)
	}

	if err := restored.RestoreSnapshot(payload[:len(payload)-1]); err == nil || (!errors.Is(err, ErrQueuePartitionOwnershipSnapshotInvalid) && !errors.Is(err, ErrQueuePartitionOwnershipSnapshotChecksum)) {
		t.Fatalf("truncated snapshot error = %v, want snapshot validation error", err)
	}
	if got := restored.Assignments(); !reflect.DeepEqual(got, before) {
		t.Fatalf("state changed after truncated restore = %+v, want %+v", got, before)
	}
}

func TestC153cQueuePartitionOwnershipSnapshotRejectsInvalidState(t *testing.T) {
	ownership, err := NewQueuePartitionOwnership(QueuePartitionOwnershipOptions{PartitionCount: 1, InitialOwners: []string{"node-a"}})
	if err != nil {
		t.Fatalf("NewQueuePartitionOwnership() error = %v", err)
	}
	for _, payload := range [][]byte{nil, []byte("wrong"), []byte("QPO1\x00\x00\x00\x00\x00\x00\x00\x00")} {
		if err := ownership.RestoreSnapshot(payload); !errors.Is(err, ErrQueuePartitionOwnershipSnapshotInvalid) && !errors.Is(err, ErrQueuePartitionOwnershipSnapshotChecksum) {
			t.Fatalf("payload %q error = %v, want snapshot validation error", payload, err)
		}
	}
}

func TestC153cQueuePartitionOwnershipSnapshotRejectsChecksummedInvalidState(t *testing.T) {
	ownership, err := NewQueuePartitionOwnership(QueuePartitionOwnershipOptions{PartitionCount: 1, InitialOwners: []string{"node-a"}})
	if err != nil {
		t.Fatalf("NewQueuePartitionOwnership() error = %v", err)
	}
	payload, err := ownership.MarshalSnapshot()
	if err != nil {
		t.Fatalf("MarshalSnapshot() error = %v", err)
	}
	stateOffset := queuePartitionOwnershipSnapshotHeaderBytes + 4 + 8 + 8
	invalidState := append([]byte(nil), payload...)
	invalidState[stateOffset] = 99
	c153cRewriteSnapshotChecksum(invalidState)
	if err := ownership.RestoreSnapshot(invalidState); !errors.Is(err, ErrQueuePartitionOwnershipSnapshotInvalid) {
		t.Fatalf("invalid state error = %v, want invalid snapshot error", err)
	}
	invalidReady := append([]byte(nil), payload...)
	invalidReady[stateOffset+1] = 2
	c153cRewriteSnapshotChecksum(invalidReady)
	if err := ownership.RestoreSnapshot(invalidReady); !errors.Is(err, ErrQueuePartitionOwnershipSnapshotInvalid) {
		t.Fatalf("invalid readiness error = %v, want invalid snapshot error", err)
	}
}

func c153cRewriteSnapshotChecksum(payload []byte) {
	binary.BigEndian.PutUint32(payload[len(payload)-queuePartitionOwnershipSnapshotChecksumBytes:], crc32.ChecksumIEEE(payload[:len(payload)-queuePartitionOwnershipSnapshotChecksumBytes]))
}

func minC153c(left, right int) int {
	if left < right {
		return left
	}
	return right
}
