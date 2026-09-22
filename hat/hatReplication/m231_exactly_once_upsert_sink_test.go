package hatReplication

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func TestExactlyOnceUpsertSinkAppliesSkipsAndRestarts(t *testing.T) {
	sink, err := NewExactlyOnceUpsertSink("orders")
	if err != nil {
		t.Fatal(err)
	}
	first := ExactlyOnceUpsertSinkRecord{Sequence: 1, OutputID: "order-1", Key: []byte("1"), Value: []byte("open")}
	decision, err := sink.Begin(first)
	if err != nil || decision.Action != ExactlyOnceUpsertSinkApply {
		t.Fatalf("first Begin() = %#v, %v", decision, err)
	}
	if _, err := sink.Commit(first.OutputID); err != nil {
		t.Fatal(err)
	}
	duplicate, err := sink.Begin(first)
	if err != nil || duplicate.Action != ExactlyOnceUpsertSinkSkip {
		t.Fatalf("duplicate Begin() = %#v, %v", duplicate, err)
	}

	second := ExactlyOnceUpsertSinkRecord{Sequence: 2, OutputID: "order-1", Key: []byte("1"), Value: []byte("paid")}
	if decision, err := sink.Begin(second); err != nil || decision.Action != ExactlyOnceUpsertSinkApply {
		t.Fatalf("second Begin() = %#v, %v", decision, err)
	}
	state, err := sink.Commit(second.OutputID)
	if err != nil {
		t.Fatal(err)
	}
	if state.CommittedSequence != 2 || state.LastOutputID != second.OutputID || state.Pending != nil {
		t.Fatalf("committed state = %#v", state)
	}

	encoded, err := state.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := NewExactlyOnceUpsertSinkFromBinary(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := restored.RestartSequence(); err != nil || got != 3 {
		t.Fatalf("restart sequence = %d/%v, want 3", got, err)
	}
	if decision, err := restored.Begin(second); err != nil || decision.Action != ExactlyOnceUpsertSinkSkip {
		t.Fatalf("restored duplicate Begin() = %#v, %v", decision, err)
	}
}

func TestExactlyOnceUpsertSinkResumesPendingAndAborts(t *testing.T) {
	sink, err := NewExactlyOnceUpsertSink("orders")
	if err != nil {
		t.Fatal(err)
	}
	pending := ExactlyOnceUpsertSinkRecord{Sequence: 1, OutputID: "order-1", Key: []byte("1"), Value: []byte("open")}
	if decision, err := sink.Begin(pending); err != nil || decision.Action != ExactlyOnceUpsertSinkApply {
		t.Fatalf("initial Begin() = %#v, %v", decision, err)
	}
	encoded, err := sink.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := NewExactlyOnceUpsertSinkFromBinary(encoded)
	if err != nil {
		t.Fatal(err)
	}
	decision, err := restarted.Begin(pending)
	if err != nil || decision.Action != ExactlyOnceUpsertSinkResume {
		t.Fatalf("resumed Begin() = %#v, %v", decision, err)
	}
	conflict := pending
	conflict.Value = []byte("tampered")
	if _, err := restarted.Begin(conflict); !errors.Is(err, ErrExactlyOnceUpsertSinkPending) {
		t.Fatalf("pending conflict error = %v", err)
	}
	if _, err := restarted.Abort(pending.OutputID); err != nil {
		t.Fatal(err)
	}
	if decision, err := restarted.Begin(pending); err != nil || decision.Action != ExactlyOnceUpsertSinkApply {
		t.Fatalf("Begin() after abort = %#v, %v", decision, err)
	}
	if _, err := restarted.Commit(pending.OutputID); err != nil {
		t.Fatal(err)
	}
}

func TestExactlyOnceUpsertSinkRejectsGapsOverlapsAndInvalidInput(t *testing.T) {
	sink, err := NewExactlyOnceUpsertSink("orders")
	if err != nil {
		t.Fatal(err)
	}
	valid := ExactlyOnceUpsertSinkRecord{Sequence: 1, OutputID: "order-1", Key: []byte("1"), Value: []byte("open")}
	tests := []struct {
		name   string
		record ExactlyOnceUpsertSinkRecord
		want   error
	}{
		{name: "zero sequence", record: ExactlyOnceUpsertSinkRecord{OutputID: "id", Key: []byte("k")}, want: ErrExactlyOnceUpsertSinkInvalid},
		{name: "missing identity", record: ExactlyOnceUpsertSinkRecord{Sequence: 1, Key: []byte("k")}, want: ErrExactlyOnceUpsertSinkInvalid},
		{name: "missing key", record: ExactlyOnceUpsertSinkRecord{Sequence: 1, OutputID: "id"}, want: ErrExactlyOnceUpsertSinkInvalid},
		{name: "gap", record: ExactlyOnceUpsertSinkRecord{Sequence: 2, OutputID: "id", Key: []byte("k")}, want: ErrExactlyOnceUpsertSinkGap},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := sink.Begin(test.record); !errors.Is(err, test.want) {
				t.Fatalf("Begin() error = %v, want %v", err, test.want)
			}
		})
	}
	if _, err := sink.Begin(valid); err != nil {
		t.Fatal(err)
	}
	if _, err := sink.Commit(valid.OutputID); err != nil {
		t.Fatal(err)
	}
	if _, err := sink.Begin(ExactlyOnceUpsertSinkRecord{Sequence: 3, OutputID: "id", Key: []byte("k")}); !errors.Is(err, ErrExactlyOnceUpsertSinkGap) {
		t.Fatalf("gap after commit error = %v", err)
	}
	if _, err := sink.Begin(ExactlyOnceUpsertSinkRecord{Sequence: 1, OutputID: "other", Key: []byte("k")}); !errors.Is(err, ErrExactlyOnceUpsertSinkConflict) {
		t.Fatalf("overlap identity error = %v", err)
	}
}

func TestExactlyOnceUpsertSinkSnapshotRoundTripAndMalformed(t *testing.T) {
	snapshot := ExactlyOnceUpsertSinkSnapshot{
		Source:               "orders",
		HasCommittedSequence: true,
		CommittedSequence:    7,
		LastOutputID:         "order-7",
		Pending: &ExactlyOnceUpsertSinkRecord{
			Sequence: 8,
			OutputID: "order-8",
			Key:      []byte("8"),
			Value:    []byte("closed"),
			Delete:   true,
		},
	}
	encoded, err := snapshot.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalExactlyOnceUpsertSinkSnapshot(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, snapshot) {
		t.Fatalf("decoded snapshot = %#v, want %#v", decoded, snapshot)
	}
	for _, malformed := range [][]byte{nil, encoded[:len(encoded)-1], append(append([]byte(nil), encoded...), 0)} {
		if _, err := UnmarshalExactlyOnceUpsertSinkSnapshot(malformed); !errors.Is(err, ErrExactlyOnceUpsertSinkSnapshotInvalid) {
			t.Fatalf("malformed snapshot error = %v", err)
		}
	}
	if bytes.Equal(encoded, []byte("")) {
		t.Fatal("snapshot encoding unexpectedly empty")
	}
}
