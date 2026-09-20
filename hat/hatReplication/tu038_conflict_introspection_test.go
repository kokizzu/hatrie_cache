package hatReplication

import (
	"bytes"
	"errors"
	"reflect"
	"sync"
	"testing"
)

func TestTU038ConflictIntrospectionBoundsAndRoundTrip(t *testing.T) {
	log, err := NewConflictIntrospectionLog(ConflictIntrospectionOptions{
		MaxEvents:     2,
		KeyHashSecret: []byte("tu038-test-secret"),
	})
	if err != nil {
		t.Fatalf("NewConflictIntrospectionLog() error = %v", err)
	}
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{})
	if err != nil {
		t.Fatalf("NewConflictPolicyRegistry() error = %v", err)
	}
	left := ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 2, NodeID: "node-b", Sequence: 1}
	for index, key := range []string{"acct-1", "acct-2", "acct-3"} {
		winner, resolveErr := registry.ResolveWithKey(log, "payments", key, left, right)
		if resolveErr != nil {
			t.Fatalf("ResolveWithKey(%q) error = %v", key, resolveErr)
		}
		if winner != right {
			t.Fatalf("ResolveWithKey(%q) winner = %#v, want %#v", key, winner, right)
		}
		if index == 0 {
			left.Timestamp++
		}
	}

	page, err := log.ReadSince(0, 10)
	if err != nil {
		t.Fatalf("ReadSince() error = %v", err)
	}
	if !page.Gap || len(page.Events) != 2 || page.Events[0].Sequence != 2 || page.Events[1].Sequence != 3 {
		t.Fatalf("ReadSince() page = %#v, want a bounded gap with sequences 2,3", page)
	}
	if page.Events[0].KeyDigest == page.Events[1].KeyDigest {
		t.Fatal("different keys produced the same digest")
	}
	if page.Events[0].Decision != ConflictIntrospectionDecisionRight {
		t.Fatalf("decision = %v, want right", page.Events[0].Decision)
	}

	wire, err := log.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	if bytes.Contains(wire, []byte("acct-1")) || bytes.Contains(wire, []byte("acct-2")) || bytes.Contains(wire, []byte("acct-3")) {
		t.Fatalf("serialized introspection log contains a raw key: %q", wire)
	}
	restored, err := NewConflictIntrospectionLog(ConflictIntrospectionOptions{
		MaxEvents:     2,
		KeyHashSecret: []byte("tu038-test-secret"),
	})
	if err != nil {
		t.Fatalf("NewConflictIntrospectionLog(restored) error = %v", err)
	}
	if err := restored.UnmarshalBinary(wire); err != nil {
		t.Fatalf("UnmarshalBinary() error = %v", err)
	}
	restoredPage, err := restored.ReadSince(0, 10)
	if err != nil {
		t.Fatalf("restored ReadSince() error = %v", err)
	}
	if !reflect.DeepEqual(restoredPage, page) {
		t.Fatalf("restored page = %#v, want %#v", restoredPage, page)
	}
}

func TestTU038ConflictIntrospectionRecordsRejectedConflicts(t *testing.T) {
	log, err := NewConflictIntrospectionLog(ConflictIntrospectionOptions{KeyHashSecret: []byte("reject-secret-1234")})
	if err != nil {
		t.Fatalf("NewConflictIntrospectionLog() error = %v", err)
	}
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{Mode: ConflictPolicyReject})
	if err != nil {
		t.Fatalf("NewConflictPolicyRegistry() error = %v", err)
	}
	left := ConflictVersion{Timestamp: 4, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 5, NodeID: "node-b", Sequence: 1}
	if _, err := registry.ResolveWithKey(log, "orders", "order-7", left, right); !errors.Is(err, ErrConflictRejected) {
		t.Fatalf("ResolveWithKey() error = %v, want ErrConflictRejected", err)
	}
	page, err := log.ReadSince(0, 10)
	if err != nil {
		t.Fatalf("ReadSince() error = %v", err)
	}
	if len(page.Events) != 1 || page.Events[0].Decision != ConflictIntrospectionDecisionRejected {
		t.Fatalf("rejected event = %#v, want one rejected event", page.Events)
	}
	if page.Events[0].Winner != (ConflictVersion{}) {
		t.Fatalf("rejected event winner = %#v, want zero", page.Events[0].Winner)
	}
}

func TestTU038ConflictIntrospectionValidatesOptionsAndPreservesDefaultResolver(t *testing.T) {
	if _, err := NewConflictIntrospectionLog(ConflictIntrospectionOptions{MaxEvents: -1, KeyHashSecret: []byte("secret")}); !errors.Is(err, ErrConflictIntrospectionOptionsInvalid) {
		t.Fatalf("negative MaxEvents error = %v, want ErrConflictIntrospectionOptionsInvalid", err)
	}
	if _, err := NewConflictIntrospectionLog(ConflictIntrospectionOptions{KeyHashSecret: nil}); !errors.Is(err, ErrConflictIntrospectionSecretRequired) {
		t.Fatalf("empty secret error = %v, want ErrConflictIntrospectionSecretRequired", err)
	}
	if _, err := NewConflictIntrospectionLog(ConflictIntrospectionOptions{KeyHashSecret: []byte("too-short")}); !errors.Is(err, ErrConflictIntrospectionSecretRequired) {
		t.Fatalf("short secret error = %v, want ErrConflictIntrospectionSecretRequired", err)
	}
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{})
	if err != nil {
		t.Fatalf("NewConflictPolicyRegistry() error = %v", err)
	}
	left := ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 2, NodeID: "node-b", Sequence: 1}
	if winner, err := registry.Resolve("payments", left, right); err != nil || winner != right {
		t.Fatalf("Resolve() = %#v/%v, want right without introspection", winner, err)
	}
}

func TestTU038ConflictIntrospectionPageCursorDoesNotSkipEvents(t *testing.T) {
	log, err := NewConflictIntrospectionLog(ConflictIntrospectionOptions{
		MaxEvents:     4,
		KeyHashSecret: []byte("cursor-secret-1234"),
	})
	if err != nil {
		t.Fatalf("NewConflictIntrospectionLog() error = %v", err)
	}
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{})
	if err != nil {
		t.Fatalf("NewConflictPolicyRegistry() error = %v", err)
	}
	left := ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 2, NodeID: "node-b", Sequence: 1}
	for _, key := range []string{"a", "b", "c"} {
		if _, err := registry.ResolveWithKey(log, "orders", key, left, right); err != nil {
			t.Fatalf("ResolveWithKey(%q) error = %v", key, err)
		}
	}
	first, err := log.ReadSince(0, 1)
	if err != nil {
		t.Fatalf("first ReadSince() error = %v", err)
	}
	if len(first.Events) != 1 || first.Events[0].Sequence != 1 || first.NextSequence != 2 {
		t.Fatalf("first page = %#v, want sequence 1 and cursor 2", first)
	}
	second, err := log.ReadSince(first.NextSequence-1, 1)
	if err != nil {
		t.Fatalf("second ReadSince() error = %v", err)
	}
	if len(second.Events) != 1 || second.Events[0].Sequence != 2 {
		t.Fatalf("second page = %#v, want sequence 2", second)
	}
}

func TestTU038ConflictIntrospectionZeroValueDoesNotPanic(t *testing.T) {
	var log ConflictIntrospectionLog
	err := log.Record(
		"orders",
		"order-1",
		ConflictVersion{Timestamp: 1, NodeID: "node-a"},
		ConflictVersion{Timestamp: 2, NodeID: "node-b"},
		ConflictPolicyLastWriteWins,
		ConflictVersion{Timestamp: 2, NodeID: "node-b"},
		ConflictIntrospectionDecisionRight,
	)
	if !errors.Is(err, ErrConflictIntrospectionOptionsInvalid) {
		t.Fatalf("zero-value Record() error = %v, want ErrConflictIntrospectionOptionsInvalid", err)
	}
}

func TestTU038ConflictIntrospectionRejectsCorruptSnapshotWithoutMutation(t *testing.T) {
	log, err := NewConflictIntrospectionLog(ConflictIntrospectionOptions{KeyHashSecret: []byte("corruption-secret")})
	if err != nil {
		t.Fatalf("NewConflictIntrospectionLog() error = %v", err)
	}
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{})
	if err != nil {
		t.Fatalf("NewConflictPolicyRegistry() error = %v", err)
	}
	left := ConflictVersion{Timestamp: 1, NodeID: "node-a"}
	right := ConflictVersion{Timestamp: 2, NodeID: "node-b"}
	if _, err := registry.ResolveWithKey(log, "orders", "order-1", left, right); err != nil {
		t.Fatalf("ResolveWithKey() error = %v", err)
	}
	wire, err := log.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	before, err := log.ReadSince(0, 10)
	if err != nil {
		t.Fatalf("ReadSince() error = %v", err)
	}
	wire[len(wire)-1] ^= 0xff
	if err := log.UnmarshalBinary(wire); !errors.Is(err, ErrConflictIntrospectionChecksum) {
		t.Fatalf("corrupt UnmarshalBinary() error = %v, want checksum error", err)
	}
	after, err := log.ReadSince(0, 10)
	if err != nil {
		t.Fatalf("ReadSince() after corruption error = %v", err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("log changed after corrupt restore: got %#v, want %#v", after, before)
	}
}

func TestTU038ConflictIntrospectionConcurrentRecordReadAndRestore(t *testing.T) {
	log, err := NewConflictIntrospectionLog(ConflictIntrospectionOptions{
		MaxEvents:     64,
		KeyHashSecret: []byte("concurrent-secret"),
	})
	if err != nil {
		t.Fatalf("NewConflictIntrospectionLog() error = %v", err)
	}
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{})
	if err != nil {
		t.Fatalf("NewConflictPolicyRegistry() error = %v", err)
	}
	left := ConflictVersion{Timestamp: 1, NodeID: "node-a"}
	right := ConflictVersion{Timestamp: 2, NodeID: "node-b"}
	var wait sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			for index := 0; index < 100; index++ {
				if _, err := registry.ResolveWithKey(log, "orders", string(rune('a'+worker))+string(rune('0'+index%10)), left, right); err != nil {
					t.Errorf("ResolveWithKey() error = %v", err)
				}
			}
		}(worker)
	}
	for reader := 0; reader < 2; reader++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for index := 0; index < 100; index++ {
				if _, err := log.ReadSince(0, 16); err != nil {
					t.Errorf("ReadSince() error = %v", err)
				}
			}
		}()
	}
	wait.Wait()
	if got := log.Stats().RetainedEvents; got != 64 {
		t.Fatalf("retained events = %d, want 64", got)
	}
}
