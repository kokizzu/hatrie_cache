package hatReplication_test

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"

	hatReplication "hatrie_cache/hat/hatReplication"
)

func TestTU38ConflictIntrospectionRoundTripAndRedaction(t *testing.T) {
	log, err := hatReplication.NewConflictIntrospectionLog(hatReplication.ConflictIntrospectionLogOptions{MaxEvents: 4})
	if err != nil {
		t.Fatalf("NewConflictIntrospectionLog() error = %v", err)
	}
	keyDigest := sha256.Sum256([]byte("customer-42"))
	first, err := log.Record(hatReplication.ConflictIntrospectionEvent{
		Space:             "orders",
		KeyDigest:         keyDigest,
		Left:              hatReplication.ConflictVersion{Timestamp: 10, NodeID: "region-a", Sequence: 1},
		Right:             hatReplication.ConflictVersion{Timestamp: 11, NodeID: "region-b", Sequence: 2},
		Decision:          hatReplication.ConflictIntrospectionRightWins,
		TimestampUnixNano: 123,
	})
	if err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	if first.Sequence != 1 {
		t.Fatalf("first sequence = %d, want 1", first.Sequence)
	}
	if _, err := log.Record(hatReplication.ConflictIntrospectionEvent{
		Space:             "orders",
		KeyDigest:         sha256.Sum256([]byte("customer-43")),
		Left:              first.Left,
		Right:             first.Right,
		Decision:          hatReplication.ConflictIntrospectionRejected,
		TimestampUnixNano: 124,
	}); err != nil {
		t.Fatalf("second Record() error = %v", err)
	}

	encoded, err := log.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	if bytes.Contains(encoded, []byte("customer-42")) || bytes.Contains(encoded, []byte("customer-43")) {
		t.Fatal("serialized conflict log contains a raw key")
	}

	restored, err := hatReplication.DecodeConflictIntrospectionLog(encoded, hatReplication.ConflictIntrospectionLogOptions{MaxEvents: 4})
	if err != nil {
		t.Fatalf("DecodeConflictIntrospectionLog() error = %v", err)
	}
	events, err := restored.Replay(0, 10)
	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if len(events) != 2 || events[0].Sequence != 1 || events[1].Sequence != 2 || events[0].Decision != hatReplication.ConflictIntrospectionRightWins {
		t.Fatalf("restored events = %#v", events)
	}
	continued, err := restored.Record(hatReplication.ConflictIntrospectionEvent{
		Space:     "orders",
		KeyDigest: sha256.Sum256([]byte("customer-44")),
		Left:      first.Left,
		Right:     first.Right,
		Decision:  hatReplication.ConflictIntrospectionLeftWins,
	})
	if err != nil {
		t.Fatalf("Record() after restore error = %v", err)
	}
	if continued.Sequence != 3 {
		t.Fatalf("sequence after restore = %d, want 3", continued.Sequence)
	}
}

func TestTU38ConflictIntrospectionHistoryGapAndLimit(t *testing.T) {
	log, err := hatReplication.NewConflictIntrospectionLog(hatReplication.ConflictIntrospectionLogOptions{MaxEvents: 2})
	if err != nil {
		t.Fatalf("NewConflictIntrospectionLog() error = %v", err)
	}
	version := hatReplication.ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	for sequence := 0; sequence < 3; sequence++ {
		if _, err := log.Record(hatReplication.ConflictIntrospectionEvent{
			Space:     "orders",
			KeyDigest: sha256.Sum256([]byte{byte(sequence)}),
			Left:      version,
			Right:     hatReplication.ConflictVersion{Timestamp: 2, NodeID: "node-b", Sequence: uint64(sequence + 2)},
			Decision:  hatReplication.ConflictIntrospectionRightWins,
		}); err != nil {
			t.Fatalf("Record(%d) error = %v", sequence, err)
		}
	}
	if _, err := log.Replay(0, 2); !errors.Is(err, hatReplication.ErrConflictIntrospectionHistoryGap) {
		t.Fatalf("Replay() error = %v, want history gap", err)
	}
	events, err := log.Replay(1, 1)
	if err != nil {
		t.Fatalf("Replay(after=1) error = %v", err)
	}
	if len(events) != 1 || events[0].Sequence != 2 {
		t.Fatalf("limited replay = %#v, want sequence 2", events)
	}
}

func TestTU38ConflictIntrospectionRejectsInvalidAndKeepsStateOnCorruption(t *testing.T) {
	log, err := hatReplication.NewConflictIntrospectionLog(hatReplication.ConflictIntrospectionLogOptions{MaxEvents: 2})
	if err != nil {
		t.Fatalf("NewConflictIntrospectionLog() error = %v", err)
	}
	valid := hatReplication.ConflictIntrospectionEvent{
		Space:     "orders",
		KeyDigest: sha256.Sum256([]byte("key")),
		Left:      hatReplication.ConflictVersion{Timestamp: 1, NodeID: "a", Sequence: 1},
		Right:     hatReplication.ConflictVersion{Timestamp: 2, NodeID: "b", Sequence: 1},
		Decision:  hatReplication.ConflictIntrospectionRightWins,
	}
	if _, err := log.Record(valid); err != nil {
		t.Fatalf("valid Record() error = %v", err)
	}
	for _, invalid := range []hatReplication.ConflictIntrospectionEvent{
		{KeyDigest: valid.KeyDigest, Left: valid.Left, Right: valid.Right, Decision: valid.Decision},
		{Space: valid.Space, KeyDigest: valid.KeyDigest, Left: hatReplication.ConflictVersion{NodeID: ""}, Right: valid.Right, Decision: valid.Decision},
		{Space: valid.Space, KeyDigest: valid.KeyDigest, Left: valid.Left, Right: valid.Right, Decision: hatReplication.ConflictIntrospectionDecision(99)},
	} {
		if _, err := log.Record(invalid); !errors.Is(err, hatReplication.ErrConflictIntrospectionInvalid) {
			t.Fatalf("Record(%#v) error = %v, want invalid", invalid, err)
		}
	}

	encoded, err := log.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	before, err := log.Replay(0, 10)
	if err != nil {
		t.Fatalf("Replay(before) error = %v", err)
	}
	encoded[len(encoded)-1] ^= 0x80
	if err := log.UnmarshalBinary(encoded); !errors.Is(err, hatReplication.ErrConflictIntrospectionChecksum) {
		t.Fatalf("UnmarshalBinary() error = %v, want checksum", err)
	}
	after, err := log.Replay(0, 10)
	if err != nil {
		t.Fatalf("Replay(after) error = %v", err)
	}
	if len(after) != len(before) || after[0].Sequence != before[0].Sequence {
		t.Fatalf("state changed after corrupt restore: before=%#v after=%#v", before, after)
	}
}

func TestTU38ConflictIntrospectionConcurrentRecordAndReplay(t *testing.T) {
	log, err := hatReplication.NewConflictIntrospectionLog(hatReplication.ConflictIntrospectionLogOptions{MaxEvents: 256})
	if err != nil {
		t.Fatalf("NewConflictIntrospectionLog() error = %v", err)
	}
	version := hatReplication.ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	var group sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			for index := 0; index < 32; index++ {
				keyDigest := sha256.Sum256([]byte{byte(worker), byte(index)})
				if _, err := log.Record(hatReplication.ConflictIntrospectionEvent{
					Space:     "orders",
					KeyDigest: keyDigest,
					Left:      version,
					Right:     hatReplication.ConflictVersion{Timestamp: 2, NodeID: "node-b", Sequence: uint64(index + 2)},
					Decision:  hatReplication.ConflictIntrospectionRightWins,
				}); err != nil {
					t.Errorf("Record() error = %v", err)
					return
				}
			}
		}(worker)
	}
	group.Wait()
	events := log.Snapshot()
	if len(events) != 256 {
		t.Fatalf("retained event count = %d, want 256", len(events))
	}
	for index := 1; index < len(events); index++ {
		if events[index].Sequence != events[index-1].Sequence+1 {
			t.Fatalf("event sequences are not contiguous at %d: %#v %#v", index, events[index-1], events[index])
		}
	}
}
