package hatReplication_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"hatrie_cache/hat/hatReplication"
)

func TestConflictEventLogRedactsKeysAndReadsWithCursor(t *testing.T) {
	log, err := hatReplication.NewConflictEventLog(hatReplication.ConflictEventLogOptions{
		Capacity: 2,
		HashSalt: []byte("test-salt"),
	})
	if err != nil {
		t.Fatal(err)
	}
	left := hatReplication.ConflictVersion{Timestamp: 1, NodeID: "west", Sequence: 1}
	right := hatReplication.ConflictVersion{Timestamp: 2, NodeID: "east", Sequence: 1}
	first, err := log.Append(hatReplication.ConflictEventRecord{
		Space:    "orders",
		Key:      "customer-secret-42",
		Left:     left,
		Right:    right,
		Winner:   right,
		Decision: hatReplication.ConflictEventDecisionRight,
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Sequence != 1 || first.KeyDigest == "" || strings.Contains(first.KeyDigest, "customer-secret-42") {
		t.Fatalf("first event = %#v", first)
	}
	second, err := log.Append(hatReplication.ConflictEventRecord{
		Space:    "orders",
		Key:      "another-key",
		Left:     left,
		Right:    right,
		Winner:   right,
		Decision: hatReplication.ConflictEventDecisionRight,
	})
	if err != nil {
		t.Fatal(err)
	}
	third, err := log.Append(hatReplication.ConflictEventRecord{
		Space:    "orders",
		Key:      "third-key",
		Left:     left,
		Right:    right,
		Winner:   right,
		Decision: hatReplication.ConflictEventDecisionRight,
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.Sequence != 2 || third.Sequence != 3 {
		t.Fatalf("event sequences = %d, %d", second.Sequence, third.Sequence)
	}
	if _, err := log.Append(hatReplication.ConflictEventRecord{
		Space:    "orders",
		Key:      "fourth-key",
		Left:     left,
		Right:    right,
		Winner:   right,
		Decision: hatReplication.ConflictEventDecisionRight,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Read(1, 1); !errors.Is(err, hatReplication.ErrConflictEventHistoryGap) {
		t.Fatalf("history-gap error = %v", err)
	}
	page, err := log.Read(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if page.OldestSequence != 3 || page.LatestSequence != 4 || len(page.Events) != 2 || page.Events[0].Sequence != 3 || page.Events[1].Sequence != 4 {
		t.Fatalf("page = %#v", page)
	}
	if page.Events[0].KeyDigest == first.KeyDigest {
		t.Fatal("evicted event unexpectedly remained in the ring")
	}
	if _, err := log.Append(hatReplication.ConflictEventRecord{
		Space:    "orders",
		Key:      "customer-secret-42",
		Left:     left,
		Right:    right,
		Winner:   right,
		Decision: hatReplication.ConflictEventDecisionRight,
	}); err != nil {
		t.Fatal(err)
	}
	latest, err := log.Read(4, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(latest.Events) != 1 || latest.Events[0].KeyDigest != first.KeyDigest {
		t.Fatalf("repeat-key page = %#v, first = %#v", latest, first)
	}
}

func TestConflictEventLogResolvesAndRestoresDeterministically(t *testing.T) {
	log, err := hatReplication.NewConflictEventLog(hatReplication.ConflictEventLogOptions{Capacity: 8, HashSalt: []byte("snapshot-salt")})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := hatReplication.NewConflictPolicyRegistry(hatReplication.ConflictPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	left := hatReplication.ConflictVersion{Timestamp: 10, NodeID: "west", Sequence: 2}
	right := hatReplication.ConflictVersion{Timestamp: 11, NodeID: "east", Sequence: 3}
	winner, err := registry.ResolveAndRecord(log, "orders", "key-1", left, right)
	if err != nil || winner != right {
		t.Fatalf("ResolveAndRecord() = %#v/%v", winner, err)
	}
	rejectLog, err := hatReplication.NewConflictEventLog(hatReplication.ConflictEventLogOptions{Capacity: 8})
	if err != nil {
		t.Fatal(err)
	}
	rejectRegistry, err := hatReplication.NewConflictPolicyRegistry(hatReplication.ConflictPolicy{Mode: hatReplication.ConflictPolicyReject})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rejectRegistry.ResolveAndRecord(rejectLog, "orders", "key-2", left, right); !errors.Is(err, hatReplication.ErrConflictRejected) {
		t.Fatalf("reject ResolveAndRecord() error = %v", err)
	}
	rejected, err := rejectLog.Read(0, 1)
	if err != nil || len(rejected.Events) != 1 || rejected.Events[0].Decision != hatReplication.ConflictEventDecisionRejected {
		t.Fatalf("rejected events = %#v/%v", rejected, err)
	}

	payload, err := log.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := hatReplication.UnmarshalConflictEventLog(payload)
	if err != nil {
		t.Fatal(err)
	}
	restoredPayload, err := restored.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload, restoredPayload) {
		t.Fatal("conflict event snapshot is not deterministic after restore")
	}
	page, err := restored.Read(0, 1)
	if err != nil || len(page.Events) != 1 || page.Events[0].WinnerSource != "east" {
		t.Fatalf("restored events = %#v/%v", page, err)
	}
	payload[len(payload)-1] ^= 0x01
	if _, err := hatReplication.UnmarshalConflictEventLog(payload); !errors.Is(err, hatReplication.ErrConflictEventLogInvalid) {
		t.Fatalf("tampered snapshot error = %v", err)
	}
}

func TestConflictEventLogRejectsInvalidInput(t *testing.T) {
	if _, err := hatReplication.NewConflictEventLog(hatReplication.ConflictEventLogOptions{Capacity: hatReplication.MaxConflictEventLogCapacity + 1}); err == nil {
		t.Fatal("excessive capacity unexpectedly accepted")
	}
	log, err := hatReplication.NewConflictEventLog(hatReplication.ConflictEventLogOptions{Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	version := hatReplication.ConflictVersion{Timestamp: 1, NodeID: "node", Sequence: 1}
	if _, err := log.Append(hatReplication.ConflictEventRecord{
		Space:    "space",
		Key:      "key",
		Left:     version,
		Right:    version,
		Winner:   version,
		Decision: hatReplication.ConflictEventDecision("unknown"),
	}); !errors.Is(err, hatReplication.ErrConflictEventLogInvalid) {
		t.Fatalf("invalid decision error = %v", err)
	}
	if _, err := log.Read(0, -1); !errors.Is(err, hatReplication.ErrConflictEventLogInvalid) {
		t.Fatalf("invalid limit error = %v", err)
	}
}
