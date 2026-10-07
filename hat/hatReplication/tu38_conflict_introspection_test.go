package hatReplication

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestTU38ConflictInspectionIsBoundedAndRedacted(t *testing.T) {
	log, err := NewConflictInspectionLog(2)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{Mode: ConflictPolicyLastWriteWins})
	if err != nil {
		t.Fatal(err)
	}
	left := ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 2, NodeID: "node-b", Sequence: 1}
	for index := 1; index <= 3; index++ {
		key := fmt.Sprintf("customer-%d", index)
		winner, err := registry.ResolveAndRecord(log, "customers", key, left, right)
		if err != nil {
			t.Fatal(err)
		}
		if winner != right {
			t.Fatalf("winner = %+v, want %+v", winner, right)
		}
	}

	page := log.Read(0, 10)
	if !page.Truncated {
		t.Fatal("Read did not report the overwritten first event")
	}
	if page.OldestSequence != 2 || page.NextSequence != 3 {
		t.Fatalf("page cursors = oldest %d next %d, want oldest 2 next 3", page.OldestSequence, page.NextSequence)
	}
	if len(page.Events) != 2 || page.Events[0].Sequence != 2 || page.Events[1].Sequence != 3 {
		t.Fatalf("events = %+v, want sequences 2 and 3", page.Events)
	}
	expectedDigest := sha256.Sum256([]byte("customer-2"))
	if page.Events[0].KeyDigest != hex.EncodeToString(expectedDigest[:]) {
		t.Fatalf("key digest = %q, want %q", page.Events[0].KeyDigest, hex.EncodeToString(expectedDigest[:]))
	}
	if page.Events[0].KeyDigest == "customer-2" {
		t.Fatal("raw key was exposed")
	}

	nextPage := log.Read(2, 1)
	if len(nextPage.Events) != 1 || nextPage.Events[0].Sequence != 3 || nextPage.NextSequence != 3 {
		t.Fatalf("next page = %+v, want sequence 3", nextPage)
	}
}

func TestTU38ConflictInspectionRecordsRejectedDecisions(t *testing.T) {
	log, err := NewConflictInspectionLog(4)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Set("orders", ConflictPolicy{Mode: ConflictPolicyReject}); err != nil {
		t.Fatal(err)
	}
	left := ConflictVersion{Timestamp: 10, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 11, NodeID: "node-b", Sequence: 1}
	_, err = registry.ResolveAndRecord(log, "orders", "order-1", left, right)
	if !errors.Is(err, ErrConflictRejected) {
		t.Fatalf("error = %v, want ErrConflictRejected", err)
	}

	events := log.Snapshot()
	if len(events) != 1 {
		t.Fatalf("event count = %d, want 1", len(events))
	}
	event := events[0]
	if event.Policy != ConflictPolicyReject || event.Decision != ConflictInspectionRejected {
		t.Fatalf("event policy/decision = %d/%q, want reject/rejected", event.Policy, event.Decision)
	}
	if event.Winner != (ConflictVersion{}) {
		t.Fatalf("rejected winner = %+v, want zero", event.Winner)
	}
}

func TestTU38ConflictInspectionPreservesSourcePriority(t *testing.T) {
	log, err := NewConflictInspectionLog(2)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{Mode: ConflictPolicySourcePriority, SourcePriority: []string{"node-b", "node-a"}})
	if err != nil {
		t.Fatal(err)
	}
	left := ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 2, NodeID: "node-b", Sequence: 1}
	winner, err := registry.ResolveAndRecord(log, "orders", "order-1", left, right)
	if err != nil {
		t.Fatal(err)
	}
	if winner != right {
		t.Fatalf("winner = %+v, want priority source %+v", winner, right)
	}
	events := log.Snapshot()
	if len(events) != 1 || events[0].Policy != ConflictPolicySourcePriority {
		t.Fatalf("events = %+v, want one source-priority event", events)
	}
}

func TestTU38ConflictInspectionNilLogPreservesResolve(t *testing.T) {
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	left := ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 2, NodeID: "node-b", Sequence: 1}
	got, err := registry.ResolveAndRecord(nil, "orders", "", left, right)
	if err != nil {
		t.Fatal(err)
	}
	want, err := registry.Resolve("orders", left, right)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("nil-log winner = %+v, want %+v", got, want)
	}
}

func TestTU38ConflictInspectionValidatesAndCopies(t *testing.T) {
	if _, err := NewConflictInspectionLog(0); !errors.Is(err, ErrConflictInspectionCapacityInvalid) {
		t.Fatalf("capacity error = %v, want ErrConflictInspectionCapacityInvalid", err)
	}
	log, err := NewConflictInspectionLog(2)
	if err != nil {
		t.Fatal(err)
	}
	version := ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	if _, err := log.Record("", "key", ConflictPolicyLastWriteWins, version, version, version, ConflictInspectionWinner); !errors.Is(err, ErrConflictInspectionSpaceRequired) {
		t.Fatalf("space error = %v, want ErrConflictInspectionSpaceRequired", err)
	}
	if _, err := log.Record("space", "", ConflictPolicyLastWriteWins, version, version, version, ConflictInspectionWinner); !errors.Is(err, ErrConflictInspectionKeyRequired) {
		t.Fatalf("key error = %v, want ErrConflictInspectionKeyRequired", err)
	}
	if _, err := log.Record("space", "key", ConflictPolicyMode(99), version, version, version, ConflictInspectionWinner); !errors.Is(err, ErrConflictInspectionPolicyInvalid) {
		t.Fatalf("policy error = %v, want ErrConflictInspectionPolicyInvalid", err)
	}

	if _, err := log.Record("space", "key", ConflictPolicyLastWriteWins, version, version, version, ConflictInspectionWinner); err != nil {
		t.Fatal(err)
	}
	snapshot := log.Snapshot()
	snapshot[0].Space = "mutated"
	if log.Snapshot()[0].Space != "space" {
		t.Fatal("Snapshot returned mutable internal storage")
	}
}

func TestTU38ConflictInspectionConcurrentAccess(t *testing.T) {
	log, err := NewConflictInspectionLog(64)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	left := ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 2, NodeID: "node-b", Sequence: 1}
	var group sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			for index := 0; index < 100; index++ {
				if _, err := registry.ResolveAndRecord(log, "space", fmt.Sprintf("key-%d-%d", worker, index), left, right); err != nil {
					t.Errorf("ResolveAndRecord error = %v", err)
					return
				}
				_ = log.Read(0, 8)
			}
		}(worker)
	}
	group.Wait()
	if len(log.Snapshot()) != 64 {
		t.Fatalf("snapshot size = %d, want capacity 64", len(log.Snapshot()))
	}
}
