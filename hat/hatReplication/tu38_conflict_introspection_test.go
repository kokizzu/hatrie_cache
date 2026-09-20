package hatReplication_test

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	hatReplication "hatrie_cache/hat/hatReplication"
)

func TestTU38ConflictIntrospectionRedactsReplaysAndRestores(t *testing.T) {
	secret := []byte("tu38-test-secret-key")
	digest, err := hatReplication.RedactConflictKey(secret, []byte("customer@example.test"))
	if err != nil {
		t.Fatalf("RedactConflictKey() error = %v", err)
	}
	if len(digest) != 64 {
		t.Fatalf("digest length = %d, want 64", len(digest))
	}
	left := hatReplication.ConflictVersion{Timestamp: 10, NodeID: "node-a", Sequence: 1}
	right := hatReplication.ConflictVersion{Timestamp: 11, NodeID: "node-b", Sequence: 2}
	registry, err := hatReplication.NewConflictPolicyRegistry(hatReplication.ConflictPolicy{})
	if err != nil {
		t.Fatalf("NewConflictPolicyRegistry() error = %v", err)
	}
	log, err := hatReplication.NewConflictIntrospectionLog(hatReplication.ConflictIntrospectionOptions{Capacity: 4})
	if err != nil {
		t.Fatalf("NewConflictIntrospectionLog() error = %v", err)
	}
	winner, err := hatReplication.ResolveConflictWithIntrospection(registry, log, "accounts", digest, left, right)
	if err != nil {
		t.Fatalf("ResolveConflictWithIntrospection() error = %v", err)
	}
	if winner != right {
		t.Fatalf("winner = %#v, want %#v", winner, right)
	}
	records, next, err := log.Replay(0, 8)
	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if next != 1 || len(records) != 1 {
		t.Fatalf("Replay() = %#v, next %d, want one record and next 1", records, next)
	}
	if records[0].KeyDigest != digest || records[0].Decision != hatReplication.ConflictIntrospectionApplied || records[0].Winner == nil || *records[0].Winner != right {
		t.Fatalf("record = %#v, want redacted applied winner", records[0])
	}
	if bytes.Contains(mustMarshalTU38(t, log), []byte("customer@example.test")) {
		t.Fatal("serialized conflict log retained the raw key")
	}

	wire, err := log.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	restored, err := hatReplication.NewConflictIntrospectionLog(hatReplication.ConflictIntrospectionOptions{Capacity: 4})
	if err != nil {
		t.Fatalf("new restore log error = %v", err)
	}
	if err := restored.RestoreBinary(wire); err != nil {
		t.Fatalf("RestoreBinary() error = %v", err)
	}
	restoredRecords, restoredNext, err := restored.Replay(0, 8)
	if err != nil {
		t.Fatalf("restored Replay() error = %v", err)
	}
	if restoredNext != next || !reflect.DeepEqual(restoredRecords, records) {
		t.Fatalf("restored records = %#v/%d, want %#v/%d", restoredRecords, restoredNext, records, next)
	}
	corrupt := append([]byte(nil), wire...)
	corrupt[len(corrupt)-1] ^= 1
	if err := restored.RestoreBinary(corrupt); !errors.Is(err, hatReplication.ErrConflictIntrospectionCorrupt) {
		t.Fatalf("RestoreBinary(corrupt) error = %v, want ErrConflictIntrospectionCorrupt", err)
	}
}

func TestTU38ConflictIntrospectionRejectsAndDetectsHistoryGaps(t *testing.T) {
	registry, err := hatReplication.NewConflictPolicyRegistry(hatReplication.ConflictPolicy{Mode: hatReplication.ConflictPolicyReject})
	if err != nil {
		t.Fatalf("NewConflictPolicyRegistry() error = %v", err)
	}
	log, err := hatReplication.NewConflictIntrospectionLog(hatReplication.ConflictIntrospectionOptions{Capacity: 2})
	if err != nil {
		t.Fatalf("NewConflictIntrospectionLog() error = %v", err)
	}
	left := hatReplication.ConflictVersion{Timestamp: 1, NodeID: "a", Sequence: 1}
	right := hatReplication.ConflictVersion{Timestamp: 2, NodeID: "b", Sequence: 1}
	for index := 0; index < 4; index++ {
		_, resolveErr := hatReplication.ResolveConflictWithIntrospection(registry, log, "space", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", left, right)
		if !errors.Is(resolveErr, hatReplication.ErrConflictRejected) {
			t.Fatalf("ResolveConflictWithIntrospection() error = %v, want rejection", resolveErr)
		}
	}
	records, next, err := log.Replay(0, 8)
	if err != nil {
		t.Fatalf("Replay(current) error = %v", err)
	}
	if len(records) != 2 || next != 4 {
		t.Fatalf("Replay(current) = %#v/%d, want two records and next 4", records, next)
	}
	if records[0].Decision != hatReplication.ConflictIntrospectionRejected || records[0].Winner != nil {
		t.Fatalf("rejected record = %#v", records[0])
	}
	if _, _, err := log.Replay(0, 0); !errors.Is(err, hatReplication.ErrConflictIntrospectionLimit) {
		t.Fatalf("Replay(zero limit) error = %v, want limit error", err)
	}
	if _, _, err := log.Replay(1, 8); !errors.Is(err, hatReplication.ErrConflictIntrospectionHistoryGap) {
		t.Fatalf("Replay(stale cursor) error = %v, want history gap", err)
	}
}

func TestTU38ConflictIntrospectionDoesNotRetainCallerMemory(t *testing.T) {
	log, err := hatReplication.NewConflictIntrospectionLog(hatReplication.ConflictIntrospectionOptions{Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	left := hatReplication.ConflictVersion{Timestamp: 1, NodeID: "a", Sequence: 1}
	right := hatReplication.ConflictVersion{Timestamp: 2, NodeID: "b", Sequence: 1}
	observation := hatReplication.ConflictIntrospectionObservation{
		Space:     "space",
		KeyDigest: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Left:      left,
		Right:     right,
		Winner:    &right,
		Decision:  hatReplication.ConflictIntrospectionApplied,
	}
	if _, err := log.Record(observation); err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	observation.Space = "changed"
	observation.Winner.NodeID = "changed"
	records, _, err := log.Replay(0, 8)
	if err != nil {
		t.Fatal(err)
	}
	if records[0].Space != "space" || records[0].Winner == nil || records[0].Winner.NodeID != "b" {
		t.Fatalf("record aliases caller memory: %#v", records[0])
	}
}

func TestTU38ConflictIntrospectionValidatesBoundsAndKeepsStateOnFailedRestore(t *testing.T) {
	if _, err := hatReplication.RedactConflictKey([]byte("short"), []byte("key")); !errors.Is(err, hatReplication.ErrConflictIntrospectionSecret) {
		t.Fatalf("short redaction secret error = %v, want ErrConflictIntrospectionSecret", err)
	}
	if _, err := hatReplication.RedactConflictKey([]byte("0123456789abcdef"), nil); !errors.Is(err, hatReplication.ErrConflictIntrospectionInvalid) {
		t.Fatalf("empty redaction key error = %v, want ErrConflictIntrospectionInvalid", err)
	}
	if _, err := hatReplication.NewConflictIntrospectionLog(hatReplication.ConflictIntrospectionOptions{Capacity: -1}); !errors.Is(err, hatReplication.ErrConflictIntrospectionCapacity) {
		t.Fatalf("negative capacity error = %v, want ErrConflictIntrospectionCapacity", err)
	}
	log, err := hatReplication.NewConflictIntrospectionLog(hatReplication.ConflictIntrospectionOptions{Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	left := hatReplication.ConflictVersion{Timestamp: 1, NodeID: "a", Sequence: 1}
	right := hatReplication.ConflictVersion{Timestamp: 2, NodeID: "b", Sequence: 1}
	if _, err := log.Record(hatReplication.ConflictIntrospectionObservation{
		Space:     "space",
		KeyDigest: "raw-key",
		Left:      left,
		Right:     right,
		Winner:    &right,
		Decision:  hatReplication.ConflictIntrospectionApplied,
	}); !errors.Is(err, hatReplication.ErrConflictIntrospectionInvalid) {
		t.Fatalf("raw key digest error = %v, want ErrConflictIntrospectionInvalid", err)
	}
	digest := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if _, err := log.Record(hatReplication.ConflictIntrospectionObservation{
		Space:     "space",
		KeyDigest: digest,
		Left:      left,
		Right:     right,
		Winner:    &right,
		Decision:  hatReplication.ConflictIntrospectionApplied,
	}); err != nil {
		t.Fatal(err)
	}
	wire, err := log.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), wire...)
	corrupt[0] = 'X'
	if err := log.RestoreBinary(corrupt); !errors.Is(err, hatReplication.ErrConflictIntrospectionCorrupt) {
		t.Fatalf("RestoreBinary(magic) error = %v, want corruption", err)
	}
	records, _, err := log.Replay(0, 8)
	if err != nil || len(records) != 1 {
		t.Fatalf("failed restore changed state: records=%#v err=%v", records, err)
	}
}

func BenchmarkTU38ConflictResolutionWithIntrospection(b *testing.B) {
	registry, err := hatReplication.NewConflictPolicyRegistry(hatReplication.ConflictPolicy{})
	if err != nil {
		b.Fatal(err)
	}
	log, err := hatReplication.NewConflictIntrospectionLog(hatReplication.ConflictIntrospectionOptions{Capacity: 1024})
	if err != nil {
		b.Fatal(err)
	}
	left := hatReplication.ConflictVersion{Timestamp: 1, NodeID: "a", Sequence: 1}
	right := hatReplication.ConflictVersion{Timestamp: 2, NodeID: "b", Sequence: 1}
	digest := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := hatReplication.ResolveConflictWithIntrospection(registry, log, "space", digest, left, right); err != nil {
			b.Fatal(err)
		}
	}
}

func mustMarshalTU38(t *testing.T, log *hatReplication.ConflictIntrospectionLog) []byte {
	t.Helper()
	wire, err := log.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return wire
}
