package hatSql

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func m232TestFrontierMessage(frontier uint64) SQLSinkFrontierMessage {
	return SQLSinkFrontierMessage{
		Sink:           "orders-sink",
		Partition:      "region-a",
		SubscriptionID: 42,
		Revision:       frontier,
		Frontier:       frontier,
		Complete:       frontier == 9,
	}
}

func TestM232SinkFrontierCheckpointRequiresTheEmittedMessage(t *testing.T) {
	coordinator := NewSQLSinkFrontierCheckpointCoordinator()
	message := m232TestFrontierMessage(5)

	if _, err := coordinator.Acknowledge(message); !errors.Is(err, ErrSQLSinkFrontierMessageNotEmitted) {
		t.Fatalf("Acknowledge(before Emit) error = %v, want not-emitted error", err)
	}
	emitted, err := coordinator.Emit(message)
	if err != nil || !emitted {
		t.Fatalf("Emit() = (%v, %v), want (true, nil)", emitted, err)
	}
	checkpoint, ok := coordinator.Checkpoint(message.Sink, message.Partition)
	if !ok || checkpoint.Message != message || checkpoint.Acknowledged {
		t.Fatalf("checkpoint after Emit() = (%#v, %v), want emitted but unacknowledged message", checkpoint, ok)
	}
	acknowledged, err := coordinator.Acknowledge(message)
	if err != nil || !acknowledged {
		t.Fatalf("Acknowledge(emitted) = (%v, %v), want (true, nil)", acknowledged, err)
	}
	acknowledged, err = coordinator.Acknowledge(message)
	if err != nil || acknowledged {
		t.Fatalf("Acknowledge(replay) = (%v, %v), want (false, nil)", acknowledged, err)
	}
	checkpoint, ok = coordinator.Checkpoint(message.Sink, message.Partition)
	if !ok || !checkpoint.Acknowledged {
		t.Fatalf("checkpoint after acknowledge = (%#v, %v), want acknowledged", checkpoint, ok)
	}
}

func TestM232SinkFrontierCheckpointRejectsOldAndConflictingMessages(t *testing.T) {
	coordinator := NewSQLSinkFrontierCheckpointCoordinator()
	message := m232TestFrontierMessage(5)
	if _, err := coordinator.Emit(message); err != nil {
		t.Fatalf("Emit(first) error = %v", err)
	}
	conflict := message
	conflict.Revision++
	if _, err := coordinator.Emit(conflict); !errors.Is(err, ErrSQLSinkFrontierMessageConflict) {
		t.Fatalf("Emit(conflict) error = %v, want conflict error", err)
	}

	newer := m232TestFrontierMessage(6)
	if _, err := coordinator.Emit(newer); err != nil {
		t.Fatalf("Emit(newer) error = %v", err)
	}
	if _, err := coordinator.Acknowledge(message); !errors.Is(err, ErrSQLSinkFrontierMessageNotEmitted) {
		t.Fatalf("Acknowledge(old) error = %v, want not-emitted error", err)
	}
	if _, err := coordinator.Acknowledge(newer); err != nil {
		t.Fatalf("Acknowledge(newer) error = %v", err)
	}
}

func TestM232SinkFrontierCheckpointRestoresUnacknowledgedMessages(t *testing.T) {
	message := m232TestFrontierMessage(7)
	first := NewSQLSinkFrontierCheckpointCoordinator()
	if _, err := first.Emit(message); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	snapshot := first.Snapshot()
	if len(snapshot) != 1 || snapshot[0].Acknowledged {
		t.Fatalf("unacknowledged snapshot = %#v, want one unacknowledged entry", snapshot)
	}

	second := NewSQLSinkFrontierCheckpointCoordinator()
	if err := second.Restore(snapshot); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	if acknowledged, err := second.Acknowledge(message); err != nil || !acknowledged {
		t.Fatalf("Acknowledge(restored) = (%v, %v), want (true, nil)", acknowledged, err)
	}
	if got := second.Snapshot(); len(got) != 1 || !got[0].Acknowledged || got[0].Message != message {
		t.Fatalf("restored acknowledged snapshot = %#v, want acknowledged message", got)
	}
}

func TestM232SinkFrontierCheckpointRestoreIsAtomicAndRejectsDuplicates(t *testing.T) {
	coordinator := NewSQLSinkFrontierCheckpointCoordinator()
	current := m232TestFrontierMessage(3)
	if _, err := coordinator.Emit(current); err != nil {
		t.Fatalf("Emit(current) error = %v", err)
	}
	before := coordinator.Snapshot()
	invalid := append([]SQLSinkFrontierCheckpoint(nil), before...)
	invalid = append(invalid, SQLSinkFrontierCheckpoint{Message: current})
	if err := coordinator.Restore(invalid); !errors.Is(err, ErrSQLSinkFrontierCheckpointDuplicate) {
		t.Fatalf("Restore(duplicate) error = %v, want duplicate error", err)
	}
	if got := coordinator.Snapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("snapshot after rejected restore = %#v, want %#v", got, before)
	}

	invalid = []SQLSinkFrontierCheckpoint{{Message: SQLSinkFrontierMessage{Sink: "", Partition: "region-a"}}}
	if err := coordinator.Restore(invalid); !errors.Is(err, ErrSQLSinkFrontierMessageInvalid) {
		t.Fatalf("Restore(invalid) error = %v, want invalid-message error", err)
	}
	if got := coordinator.Snapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("snapshot after invalid restore = %#v, want %#v", got, before)
	}
}

func TestM232NewSinkFrontierMessageFromBatchValidatesProgressFrame(t *testing.T) {
	batch := QuerySubscriptionDeltaBatch{ID: 9, Revision: 2, Frontier: 11, Progress: true, Complete: true}
	message, err := NewSQLSinkFrontierMessageFromBatch("sink", "partition", batch)
	if err != nil {
		t.Fatalf("NewSQLSinkFrontierMessageFromBatch() error = %v", err)
	}
	want := SQLSinkFrontierMessage{
		Sink:           "sink",
		Partition:      "partition",
		SubscriptionID: 9,
		Revision:       2,
		Frontier:       11,
		Complete:       true,
	}
	if message != want {
		t.Fatalf("message = %#v, want %#v", message, want)
	}
	for _, invalid := range []QuerySubscriptionDeltaBatch{
		{ID: 9, Revision: 2, Frontier: 11},
		{ID: 9, Revision: 2, Frontier: 11, Progress: true, Reset: true},
		{ID: 9, Revision: 2, Frontier: 11, Progress: true, Columns: []string{"id"}},
		{ID: 9, Revision: 2, Frontier: 11, Progress: true, Deltas: []QuerySubscriptionDelta{{Diff: 1}}},
	} {
		if _, err := NewSQLSinkFrontierMessageFromBatch("sink", "partition", invalid); !errors.Is(err, ErrSQLSinkFrontierMessageInvalid) {
			t.Fatalf("invalid batch %#v error = %v, want invalid-message error", invalid, err)
		}
	}
}

func TestM232SinkFrontierCheckpointValidatesNilAndMalformedInput(t *testing.T) {
	var coordinator *SQLSinkFrontierCheckpointCoordinator
	message := m232TestFrontierMessage(1)
	if _, err := coordinator.Emit(message); !errors.Is(err, ErrSQLSinkFrontierCheckpointNil) {
		t.Fatalf("nil Emit() error = %v, want nil-checkpoint error", err)
	}
	if _, err := NewSQLSinkFrontierMessageFromBatch("", "partition", QuerySubscriptionDeltaBatch{Progress: true}); !errors.Is(err, ErrSQLSinkFrontierMessageInvalid) {
		t.Fatalf("malformed message error = %v, want invalid-message error", err)
	}
	if _, err := NewSQLSinkFrontierMessageFromBatch(strings.Repeat("s", MaxSQLSinkFrontierMessageNameBytes+1), "partition", QuerySubscriptionDeltaBatch{ID: 1, Revision: 1, Progress: true}); !errors.Is(err, ErrSQLSinkFrontierMessageInvalid) {
		t.Fatalf("oversized sink name error = %v, want invalid-message error", err)
	}
}
