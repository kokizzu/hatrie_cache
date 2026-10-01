package hatReplication_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"hatrie_cache/hat/hatReplication"
)

func TestConflictEventLogRedactsKeysAndReadsCursor(t *testing.T) {
	log, err := hatReplication.NewConflictEventLog(hatReplication.ConflictEventLogOptions{Capacity: 2})
	if err != nil {
		t.Fatalf("NewConflictEventLog() error = %v", err)
	}
	left := hatReplication.ConflictVersion{Timestamp: 10, NodeID: "node-a", Sequence: 1}
	right := hatReplication.ConflictVersion{Timestamp: 11, NodeID: "node-b", Sequence: 2}
	winner := right
	event, err := log.Append(hatReplication.ConflictEventInput{
		Space:      "orders",
		Key:        []byte("customer-secret-7"),
		Left:       left,
		Right:      right,
		Winner:     winner,
		Decision:   hatReplication.ConflictDecisionLastWriteWins,
		ObservedAt: time.Unix(1700000000, 123).UTC(),
	})
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	wantDigest := sha256.Sum256([]byte("customer-secret-7"))
	if event.KeyDigest != hex.EncodeToString(wantDigest[:]) {
		t.Fatalf("event key digest = %q, want %x", event.KeyDigest, wantDigest)
	}
	if strings.Contains(event.KeyDigest, "customer-secret-7") {
		t.Fatalf("event key digest contains plaintext key: %q", event.KeyDigest)
	}
	batch := log.Read(0, 10)
	if batch.HistoryGap || len(batch.Events) != 1 || batch.NextID != event.ID {
		t.Fatalf("Read() = %#v, want one event without gap", batch)
	}
	if got := batch.Events[0]; got != event {
		t.Fatalf("Read() event = %#v, want %#v", got, event)
	}
}

func TestConflictEventLogBoundsHistoryAndRestoresChecksummedSnapshot(t *testing.T) {
	log, err := hatReplication.NewConflictEventLog(hatReplication.ConflictEventLogOptions{Capacity: 2})
	if err != nil {
		t.Fatalf("NewConflictEventLog() error = %v", err)
	}
	for index := 0; index < 3; index++ {
		if _, err := log.Append(hatReplication.ConflictEventInput{
			Space:    "orders",
			Key:      []byte{byte(index + 1)},
			Left:     hatReplication.ConflictVersion{Timestamp: int64(index + 1), NodeID: "node-a", Sequence: uint64(index + 1)},
			Right:    hatReplication.ConflictVersion{Timestamp: int64(index + 2), NodeID: "node-b", Sequence: uint64(index + 1)},
			Winner:   hatReplication.ConflictVersion{Timestamp: int64(index + 2), NodeID: "node-b", Sequence: uint64(index + 1)},
			Decision: hatReplication.ConflictDecisionSourcePriority,
		}); err != nil {
			t.Fatalf("Append(%d) error = %v", index, err)
		}
	}
	batch := log.Read(0, 10)
	if !batch.HistoryGap || len(batch.Events) != 2 || batch.Events[0].ID != 2 || batch.Events[1].ID != 3 {
		t.Fatalf("bounded Read() = %#v, want ids 2 and 3 with history gap", batch)
	}
	wire, err := log.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	if bytes.Contains(wire, []byte("orders")) == false {
		t.Fatal("snapshot should retain space metadata")
	}
	if bytes.Contains(wire, []byte("customer-secret-7")) {
		t.Fatal("snapshot contains plaintext key")
	}
	restored, err := hatReplication.NewConflictEventLogFromBinary(wire)
	if err != nil {
		t.Fatalf("NewConflictEventLogFromBinary() error = %v", err)
	}
	if got, want := restored.Read(0, 10), batch; got.NextID != want.NextID || got.HistoryGap != want.HistoryGap || len(got.Events) != len(want.Events) || got.Events[0] != want.Events[0] || got.Events[1] != want.Events[1] {
		t.Fatalf("restored Read() = %#v, want %#v", got, want)
	}
	wire[len(wire)-1] ^= 1
	if _, err := hatReplication.NewConflictEventLogFromBinary(wire); !errors.Is(err, hatReplication.ErrConflictEventSnapshotCorrupt) {
		t.Fatalf("corrupted snapshot error = %v, want ErrConflictEventSnapshotCorrupt", err)
	}
}

func TestConflictEventLogWaitAndValidation(t *testing.T) {
	log, err := hatReplication.NewConflictEventLog(hatReplication.ConflictEventLogOptions{Capacity: 1})
	if err != nil {
		t.Fatalf("NewConflictEventLog() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := log.Wait(ctx, 0, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait() error = %v, want context.Canceled", err)
	}
	if _, err := log.Append(hatReplication.ConflictEventInput{Space: "", Key: []byte("key")}); !errors.Is(err, hatReplication.ErrConflictEventSpaceRequired) {
		t.Fatalf("empty space error = %v, want ErrConflictEventSpaceRequired", err)
	}
	if _, err := log.Append(hatReplication.ConflictEventInput{Space: "orders", Key: []byte("key"), Left: hatReplication.ConflictVersion{NodeID: "node-a"}}); !errors.Is(err, hatReplication.ErrConflictEventVersionInvalid) {
		t.Fatalf("incomplete version error = %v, want ErrConflictEventVersionInvalid", err)
	}
}

func TestConflictEventLogWaitReturnsAppendedEvent(t *testing.T) {
	log, err := hatReplication.NewConflictEventLog(hatReplication.ConflictEventLogOptions{Capacity: 2})
	if err != nil {
		t.Fatalf("NewConflictEventLog() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	type waitResult struct {
		batch hatReplication.ConflictEventBatch
		err   error
	}
	results := make(chan waitResult, 1)
	go func() {
		batch, waitErr := log.Wait(ctx, 0, 1)
		results <- waitResult{batch: batch, err: waitErr}
	}()
	time.Sleep(time.Millisecond)
	if _, err := log.Append(hatReplication.ConflictEventInput{
		Space:    "orders",
		Key:      []byte("key"),
		Left:     hatReplication.ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1},
		Right:    hatReplication.ConflictVersion{Timestamp: 2, NodeID: "node-b", Sequence: 1},
		Winner:   hatReplication.ConflictVersion{Timestamp: 2, NodeID: "node-b", Sequence: 1},
		Decision: hatReplication.ConflictDecisionLastWriteWins,
	}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	select {
	case result := <-results:
		if result.err != nil || len(result.batch.Events) != 1 || result.batch.Events[0].ID != 1 {
			t.Fatalf("Wait() = %#v, want event id 1", result)
		}
	case <-ctx.Done():
		t.Fatalf("Wait() did not wake after Append(): %v", ctx.Err())
	}
}
