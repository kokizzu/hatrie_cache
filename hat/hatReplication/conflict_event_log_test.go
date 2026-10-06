package hatReplication

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"
)

func TestConflictEventLogAppendsRedactedEventsAndReplaysByCursor(t *testing.T) {
	log, err := NewConflictEventLog(2)
	if err != nil {
		t.Fatal(err)
	}
	digestA := [16]byte{1}
	digestB := [16]byte{2}
	left := ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 2, NodeID: "node-b", Sequence: 1}
	first, err := log.Append(ConflictEvent{
		Space:     "orders",
		KeyDigest: digestA,
		Left:      left,
		Right:     right,
		Winner:    right,
		Decision:  ConflictEventDecisionRight,
		Policy:    ConflictPolicyLastWriteWins,
	})
	if err != nil || first != 1 {
		t.Fatalf("first append = %d, %v, want sequence 1", first, err)
	}
	second, err := log.Append(ConflictEvent{
		Space:     "orders",
		KeyDigest: digestB,
		Left:      right,
		Right:     left,
		Winner:    right,
		Decision:  ConflictEventDecisionLeft,
		Policy:    ConflictPolicySourcePriority,
	})
	if err != nil || second != 2 {
		t.Fatalf("second append = %d, %v, want sequence 2", second, err)
	}
	third, err := log.Append(ConflictEvent{
		Space:     "orders",
		KeyDigest: [16]byte{3},
		Left:      left,
		Right:     right,
		Decision:  ConflictEventDecisionRejected,
		Policy:    ConflictPolicyReject,
	})
	if err != nil || third != 3 {
		t.Fatalf("third append = %d, %v, want sequence 3", third, err)
	}
	fourth, err := log.Append(ConflictEvent{
		Space:     "orders",
		KeyDigest: [16]byte{4},
		Left:      left,
		Right:     right,
		Winner:    left,
		Decision:  ConflictEventDecisionLeft,
		Policy:    ConflictPolicyLastWriteWins,
	})
	if err != nil || fourth != 4 {
		t.Fatalf("fourth append = %d, %v, want sequence 4", fourth, err)
	}

	events, next, err := log.Read(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || next != 4 {
		t.Fatalf("Read(0) = %d events, next %d, want 2 and 4", len(events), next)
	}
	if events[0].Sequence != 3 || events[0].Decision != ConflictEventDecisionRejected {
		t.Fatalf("first retained event = %#v, want rejected sequence 3", events[0])
	}
	if events[0].Winner != (ConflictVersion{}) || events[1].Sequence != 4 || events[1].KeyDigest != [16]byte{4} {
		t.Fatalf("retained events = %#v, want rejected sequence 3 and digest 4", events)
	}

	if _, _, err := log.Read(1, 1); !errors.Is(err, ErrConflictEventCursorStale) {
		t.Fatalf("stale cursor error = %v, want ErrConflictEventCursorStale", err)
	}
	if got, next, err := log.Read(2, 1); err != nil || len(got) != 1 || got[0].Sequence != 3 || next != 3 {
		t.Fatalf("Read(2) = %#v, next %d, %v, want sequence 3", got, next, err)
	}
}

func TestConflictEventLogSnapshotRestoreAndValidation(t *testing.T) {
	log, err := NewConflictEventLog(4)
	if err != nil {
		t.Fatal(err)
	}
	version := ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	if _, err := log.Append(ConflictEvent{
		Space:     "payments",
		KeyDigest: [16]byte{9},
		Left:      version,
		Right:     ConflictVersion{Timestamp: 2, NodeID: "node-b", Sequence: 1},
		Winner:    version,
		Decision:  ConflictEventDecisionLeft,
		Policy:    ConflictPolicyLastWriteWins,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot := log.Snapshot()
	restored, err := NewConflictEventLogFromSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	got, next, err := restored.Read(0, 0)
	if err != nil || len(got) != 1 || got[0].Space != "payments" || next != 1 {
		t.Fatalf("restored Read() = %#v, next %d, %v", got, next, err)
	}
	if _, err := NewConflictEventLogFromSnapshot(ConflictEventLogSnapshot{Capacity: 1, LastSequence: 2, Events: []ConflictEvent{{Sequence: 2}}}); !errors.Is(err, ErrConflictEventSnapshotInvalid) {
		t.Fatalf("invalid snapshot error = %v, want ErrConflictEventSnapshotInvalid", err)
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var decoded ConflictEventLogSnapshot
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if _, err := NewConflictEventLogFromSnapshot(decoded); err != nil {
		t.Fatalf("JSON-restored snapshot error = %v", err)
	}
}

func TestConflictEventLogRejectsInvalidEvents(t *testing.T) {
	log, err := NewConflictEventLog(1)
	if err != nil {
		t.Fatal(err)
	}
	valid := ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	cases := []ConflictEvent{
		{Space: "", Left: valid, Right: valid, Winner: valid, Decision: ConflictEventDecisionLeft},
		{Space: "orders", Left: valid, Right: valid, Winner: valid, Decision: ConflictEventDecision(99)},
		{Space: "orders", Left: valid, Right: valid, Winner: ConflictVersion{}, Decision: ConflictEventDecisionLeft},
		{Space: "orders", Left: valid, Right: valid, Winner: valid, Decision: ConflictEventDecisionRejected},
	}
	for index, event := range cases {
		if _, err := log.Append(event); !errors.Is(err, ErrConflictEventInvalid) {
			t.Fatalf("case %d error = %v, want ErrConflictEventInvalid", index, err)
		}
	}
}

func TestConflictEventLogConcurrentAppendKeepsBoundedContiguousWindow(t *testing.T) {
	log, err := NewConflictEventLog(32)
	if err != nil {
		t.Fatal(err)
	}
	left := ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 2, NodeID: "node-b", Sequence: 1}
	var group sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		worker := worker
		group.Add(1)
		go func() {
			defer group.Done()
			for index := 0; index < 50; index++ {
				if _, err := log.Append(ConflictEvent{
					Space:     "orders",
					KeyDigest: [16]byte{byte(worker), byte(index)},
					Left:      left,
					Right:     right,
					Winner:    right,
					Decision:  ConflictEventDecisionRight,
					Policy:    ConflictPolicyLastWriteWins,
				}); err != nil {
					t.Errorf("Append() error = %v", err)
					return
				}
			}
		}()
	}
	group.Wait()
	snapshot := log.Snapshot()
	if len(snapshot.Events) != 32 || snapshot.LastSequence != 200 {
		t.Fatalf("snapshot = %d events, last sequence %d, want 32 and 200", len(snapshot.Events), snapshot.LastSequence)
	}
	for index, event := range snapshot.Events {
		want := snapshot.LastSequence - uint64(len(snapshot.Events)) + 1 + uint64(index)
		if event.Sequence != want {
			t.Fatalf("event %d sequence = %d, want %d", index, event.Sequence, want)
		}
	}
}
