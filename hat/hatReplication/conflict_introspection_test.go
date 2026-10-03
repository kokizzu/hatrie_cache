package hatReplication

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

func conflictTestEvent(space string, value byte) ConflictEvent {
	event := ConflictEvent{Space: space, WinnerSource: "node-a", LoserSource: "node-b", Decision: ConflictDecisionLastWriteWins, Timestamp: 42}
	event.KeyDigest[0] = value
	return event
}

func TestConflictEventLogAppendsAndReadsInSequenceOrder(t *testing.T) {
	log, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 4})
	if err != nil {
		t.Fatal(err)
	}
	for index := byte(1); index <= 3; index++ {
		if _, err := log.Append(conflictTestEvent("orders", index)); err != nil {
			t.Fatal(err)
		}
	}

	events, err := log.ReadAfter(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 || events[0].Sequence != 1 || events[2].Sequence != 3 || events[1].KeyDigest[0] != 2 {
		t.Fatalf("events = %#v, want sequences 1..3", events)
	}
	events[0].Space = "mutated"
	snapshot := log.Snapshot()
	if snapshot[0].Space != "orders" {
		t.Fatalf("read result mutated log: %#v", snapshot)
	}
}

func TestConflictEventLogRetainsBoundedHistoryAndReportsGap(t *testing.T) {
	log, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	for index := byte(1); index <= 3; index++ {
		if _, err := log.Append(conflictTestEvent("orders", index)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := log.ReadAfter(0, 10); !errors.Is(err, ErrConflictEventHistoryGap) {
		t.Fatalf("ReadAfter gap error = %v, want ErrConflictEventHistoryGap", err)
	}
	events, err := log.ReadAfter(1, 10)
	if err != nil || len(events) != 2 || events[0].Sequence != 2 || events[1].Sequence != 3 {
		t.Fatalf("retained events = %#v, err = %v", events, err)
	}
	stats := log.Stats()
	if stats.FirstSequence != 2 || stats.NextSequence != 4 || stats.Retained != 2 || stats.Dropped != 1 {
		t.Fatalf("stats = %#v, want first=2 next=4 retained=2 dropped=1", stats)
	}
}

func TestConflictEventLogWaitAfterWakesOnAppendAndHonorsCancellation(t *testing.T) {
	log, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 4})
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if _, err := log.WaitAfter(cancelled, 0, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitAfter cancellation error = %v", err)
	}

	go func() {
		time.Sleep(time.Millisecond)
		_, _ = log.Append(conflictTestEvent("orders", 1))
	}()
	events, err := log.WaitAfter(context.Background(), 0, 1)
	if err != nil || len(events) != 1 || events[0].Sequence != 1 {
		t.Fatalf("WaitAfter events = %#v, err = %v", events, err)
	}
}

func TestConflictEventLogBinarySnapshotRoundTripAndRedaction(t *testing.T) {
	log, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 4})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(conflictTestEvent("orders", 7)); err != nil {
		t.Fatal(err)
	}
	encoded, err := log.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte("raw-secret-key")) {
		t.Fatal("binary snapshot contains a raw key")
	}

	restored, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.UnmarshalBinary(encoded); err != nil {
		t.Fatal(err)
	}
	if got := restored.Snapshot(); len(got) != 1 || got[0].KeyDigest[0] != 7 || got[0].Sequence != 1 {
		t.Fatalf("restored events = %#v", got)
	}
}

func TestConflictEventLogRejectsInvalidEventsAndSnapshots(t *testing.T) {
	log, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	invalid := conflictTestEvent("orders", 1)
	invalid.Space = ""
	if _, err := log.Append(invalid); !errors.Is(err, ErrConflictEventSpaceRequired) {
		t.Fatalf("empty space error = %v", err)
	}
	invalid = conflictTestEvent("orders", 1)
	invalid.KeyDigest = [ConflictEventDigestSize]byte{}
	if _, err := log.Append(invalid); !errors.Is(err, ErrConflictEventKeyDigestRequired) {
		t.Fatalf("empty digest error = %v", err)
	}
	invalid = conflictTestEvent("orders", 1)
	invalid.Decision = "unknown"
	if _, err := log.Append(invalid); !errors.Is(err, ErrConflictEventDecisionInvalid) {
		t.Fatalf("invalid decision error = %v", err)
	}
	if err := log.UnmarshalBinary([]byte("not-a-conflict-snapshot")); !errors.Is(err, ErrConflictEventSnapshotInvalid) {
		t.Fatalf("invalid snapshot error = %v", err)
	}
	if got := log.Snapshot(); len(got) != 0 {
		t.Fatalf("invalid snapshot mutated log = %#v", got)
	}
}

func TestConflictEventLogRestoresEmptySnapshotForFutureAppends(t *testing.T) {
	original, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := original.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.UnmarshalBinary(encoded); err != nil {
		t.Fatal(err)
	}
	if _, err := restored.Append(conflictTestEvent("orders", 9)); err != nil {
		t.Fatal(err)
	}
	if got := restored.Snapshot(); len(got) != 1 || got[0].Sequence != 1 {
		t.Fatalf("restored append = %#v", got)
	}
}
