package hatReplication_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"hatrie_cache/hat/hatReplication"
)

func TestTU38ConflictEventLogRecordsRedactedPolicyOutcomes(t *testing.T) {
	log, err := hatReplication.NewConflictEventLog(4)
	if err != nil {
		t.Fatalf("NewConflictEventLog() error = %v", err)
	}
	registry, err := hatReplication.NewConflictPolicyRegistry(hatReplication.ConflictPolicy{})
	if err != nil {
		t.Fatalf("NewConflictPolicyRegistry() error = %v", err)
	}
	registry.SetEventLog(log)

	left := hatReplication.ConflictVersion{Timestamp: 10, NodeID: "node-a", Sequence: 3}
	right := hatReplication.ConflictVersion{Timestamp: 11, NodeID: "node-b", Sequence: 4}
	winner, err := registry.Resolve("payments", left, right)
	if err != nil || winner != right {
		t.Fatalf("Resolve() = %#v, %v; want right winner", winner, err)
	}

	if err := registry.Set("strict", hatReplication.ConflictPolicy{Mode: hatReplication.ConflictPolicyReject}); err != nil {
		t.Fatalf("Set(strict) error = %v", err)
	}
	if _, err := registry.Resolve("strict", left, right); !errors.Is(err, hatReplication.ErrConflictRejected) {
		t.Fatalf("strict Resolve() error = %v, want ErrConflictRejected", err)
	}

	events, err := log.Read(0, 0)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("event count = %d, want 2", len(events))
	}
	if events[0].Space != "payments" || events[0].Outcome != hatReplication.ConflictEventResolved || events[0].Winner != right {
		t.Fatalf("resolved event = %#v", events[0])
	}
	if events[1].Space != "strict" || events[1].Outcome != hatReplication.ConflictEventRejected || events[1].Winner != (hatReplication.ConflictVersion{}) {
		t.Fatalf("rejected event = %#v", events[1])
	}
	if events[0].Sequence != 1 || events[1].Sequence != 2 {
		t.Fatalf("event sequences = %d, %d; want 1, 2", events[0].Sequence, events[1].Sequence)
	}
}

func TestTU38ConflictEventLogBoundsHistoryAndWaits(t *testing.T) {
	log, err := hatReplication.NewConflictEventLog(2)
	if err != nil {
		t.Fatalf("NewConflictEventLog() error = %v", err)
	}
	registry, err := hatReplication.NewConflictPolicyRegistry(hatReplication.ConflictPolicy{})
	if err != nil {
		t.Fatalf("NewConflictPolicyRegistry() error = %v", err)
	}
	registry.SetEventLog(log)
	left := hatReplication.ConflictVersion{Timestamp: 1, NodeID: "left", Sequence: 1}
	right := hatReplication.ConflictVersion{Timestamp: 2, NodeID: "right", Sequence: 1}
	for index := 0; index < 3; index++ {
		if _, err := registry.Resolve("space", left, right); err != nil {
			t.Fatalf("Resolve(%d) error = %v", index, err)
		}
	}
	if _, err := log.Read(0, 0); !errors.Is(err, hatReplication.ErrConflictEventCursorExpired) {
		t.Fatalf("expired Read() error = %v, want ErrConflictEventCursorExpired", err)
	}
	events, err := log.Read(1, 0)
	if err != nil || len(events) != 2 || events[0].Sequence != 2 || events[1].Sequence != 3 {
		t.Fatalf("Read(after=1) = %#v, %v", events, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	waitDone := make(chan error, 1)
	go func() { waitDone <- log.Wait(ctx, 3) }()
	select {
	case err := <-waitDone:
		t.Fatalf("Wait() returned before a new event: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	if _, err := registry.Resolve("space", left, right); err != nil {
		t.Fatalf("post-wait Resolve() error = %v", err)
	}
	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatalf("Wait() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Wait() did not wake after recording an event")
	}
}

func TestTU38ConflictEventLogIsDisabledByDefault(t *testing.T) {
	registry, err := hatReplication.NewConflictPolicyRegistry(hatReplication.ConflictPolicy{})
	if err != nil {
		t.Fatalf("NewConflictPolicyRegistry() error = %v", err)
	}
	if registry.EventLog() != nil {
		t.Fatal("new registry has an event log; default path must be disabled")
	}
	_, _ = registry.Resolve("space", hatReplication.ConflictVersion{Timestamp: 1, NodeID: "a"}, hatReplication.ConflictVersion{Timestamp: 2, NodeID: "b"})
}

func TestTU38ConflictEventLogValidatesCapacityAndClose(t *testing.T) {
	for _, capacity := range []int{-1, hatReplication.MaxConflictEventCapacity + 1} {
		if _, err := hatReplication.NewConflictEventLog(capacity); !errors.Is(err, hatReplication.ErrConflictEventCapacityInvalid) {
			t.Fatalf("NewConflictEventLog(%d) error = %v, want capacity error", capacity, err)
		}
	}
	log, err := hatReplication.NewConflictEventLog(1)
	if err != nil {
		t.Fatalf("NewConflictEventLog() error = %v", err)
	}
	log.Close()
	if err := log.Wait(context.Background(), 0); !errors.Is(err, hatReplication.ErrConflictEventLogClosed) {
		t.Fatalf("Wait() after Close() error = %v, want close error", err)
	}
}
