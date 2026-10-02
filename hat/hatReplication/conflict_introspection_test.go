package hatReplication

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"
)

func TestConflictIntrospectionLogRedactsAndRestores(t *testing.T) {
	now := time.Unix(123, 456)
	log, err := NewConflictIntrospectionLog(ConflictIntrospectionLogOptions{
		Capacity: 2,
		Now:      func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewConflictIntrospectionLog() error = %v", err)
	}

	left := ConflictVersion{Timestamp: 10, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 11, NodeID: "node-b", Sequence: 2}
	key := []byte("customer-42")
	first, err := log.Record(ConflictIntrospectionInput{
		Space:   "orders",
		Key:     key,
		Left:    left,
		Right:   right,
		Winner:  right,
		Outcome: ConflictIntrospectionRightWon,
	})
	if err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	key[0] = 'X'

	digest := sha256.Sum256([]byte("customer-42"))
	if first.Sequence != 1 || first.AtUnixNano != now.UnixNano() || first.KeyDigest != hex.EncodeToString(digest[:16]) {
		t.Fatalf("record = %#v, want sequence, timestamp, and digest", first)
	}
	if first.KeyDigest == "customer-42" {
		t.Fatal("record retained the raw key")
	}

	if _, err := log.Record(ConflictIntrospectionInput{
		Space:   "orders",
		Key:     []byte("customer-43"),
		Left:    right,
		Right:   left,
		Outcome: ConflictIntrospectionRejected,
	}); err != nil {
		t.Fatalf("Record(rejected) error = %v", err)
	}

	records, next, err := log.Read(0, 10)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(records) != 2 || next != 2 || records[0].Sequence != 1 || records[1].Outcome != ConflictIntrospectionRejected {
		t.Fatalf("Read() = %#v, next %d", records, next)
	}

	wire, err := log.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	restored, err := UnmarshalConflictIntrospectionLog(wire)
	if err != nil {
		t.Fatalf("UnmarshalConflictIntrospectionLog() error = %v", err)
	}
	restoredRecords, restoredNext, err := restored.Read(0, 10)
	if err != nil {
		t.Fatalf("restored Read() error = %v", err)
	}
	if restoredNext != next || len(restoredRecords) != len(records) || restoredRecords[0] != records[0] || restoredRecords[1] != records[1] {
		t.Fatalf("restored records = %#v, next %d; want %#v, next %d", restoredRecords, restoredNext, records, next)
	}

	wireAgain, err := restored.MarshalBinary()
	if err != nil {
		t.Fatalf("restored MarshalBinary() error = %v", err)
	}
	if !bytes.Equal(wire, wireAgain) {
		t.Fatalf("snapshot encoding is not deterministic: %x != %x", wire, wireAgain)
	}
	corrupt := append([]byte(nil), wire...)
	corrupt[len(corrupt)-1] ^= 1
	if _, err := UnmarshalConflictIntrospectionLog(corrupt); !errors.Is(err, ErrConflictIntrospectionInvalid) {
		t.Fatalf("Unmarshal(corrupt) error = %v, want ErrConflictIntrospectionInvalid", err)
	}
}

func TestConflictIntrospectionLogBoundsAndRejectsExpiredCursor(t *testing.T) {
	log, err := NewConflictIntrospectionLog(ConflictIntrospectionLogOptions{Capacity: 2})
	if err != nil {
		t.Fatalf("NewConflictIntrospectionLog() error = %v", err)
	}
	version := ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	for index := 0; index < 3; index++ {
		if _, err := log.Record(ConflictIntrospectionInput{
			Space:   "orders",
			Key:     []byte{byte(index + 1)},
			Left:    version,
			Right:   version,
			Winner:  version,
			Outcome: ConflictIntrospectionLeftWon,
		}); err != nil {
			t.Fatalf("Record(%d) error = %v", index, err)
		}
	}
	if _, _, err := log.Read(0, 10); !errors.Is(err, ErrConflictIntrospectionCursorExpired) {
		t.Fatalf("Read(expired) error = %v, want ErrConflictIntrospectionCursorExpired", err)
	}
	records, next, err := log.Read(1, 10)
	if err != nil || len(records) != 2 || next != 3 || records[0].Sequence != 2 {
		t.Fatalf("Read(after 1) = %#v, next %d, error %v", records, next, err)
	}

	for _, input := range []ConflictIntrospectionInput{
		{Space: "", Key: []byte("key"), Left: version, Right: version, Winner: version, Outcome: ConflictIntrospectionLeftWon},
		{Space: "orders", Key: nil, Left: version, Right: version, Winner: version, Outcome: ConflictIntrospectionLeftWon},
		{Space: "orders", Key: []byte("key"), Left: version, Right: version, Outcome: ConflictIntrospectionLeftWon},
	} {
		if _, err := log.Record(input); !errors.Is(err, ErrConflictIntrospectionInvalid) {
			t.Fatalf("Record(%#v) error = %v, want ErrConflictIntrospectionInvalid", input, err)
		}
	}

	corrupt := []byte{1, 2, 3}
	if _, err := UnmarshalConflictIntrospectionLog(corrupt); !errors.Is(err, ErrConflictIntrospectionInvalid) {
		t.Fatalf("Unmarshal(corrupt) error = %v, want ErrConflictIntrospectionInvalid", err)
	}
	var zero ConflictIntrospectionLog
	if _, err := zero.Record(ConflictIntrospectionInput{Space: "orders", Key: []byte("key"), Left: version, Right: version, Winner: version, Outcome: ConflictIntrospectionLeftWon}); !errors.Is(err, ErrConflictIntrospectionInvalid) {
		t.Fatalf("zero-value Record() error = %v, want ErrConflictIntrospectionInvalid", err)
	}
}
