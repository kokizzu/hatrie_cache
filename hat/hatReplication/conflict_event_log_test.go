package hatReplication

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestConflictEventLogRedactsAndReplays(t *testing.T) {
	log, err := NewConflictEventLog(ConflictEventLogOptions{
		Capacity: 4,
		Secret:   []byte("conflict-event-test-secret"),
	})
	if err != nil {
		t.Fatalf("NewConflictEventLog() error = %v", err)
	}
	left := ConflictVersion{Timestamp: 10, NodeID: "west", Sequence: 1}
	right := ConflictVersion{Timestamp: 11, NodeID: "east", Sequence: 2}
	event, err := log.Record(ConflictEventInput{
		Space:    "orders",
		Key:      []byte("customer-42"),
		Left:     left,
		Right:    right,
		Decision: ConflictEventDecisionRightWins,
		Winner:   &right,
	})
	if err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	if event.Sequence != 1 || event.Space != "orders" || event.Decision != ConflictEventDecisionRightWins {
		t.Fatalf("event = %#v", event)
	}
	if event.KeyFingerprint == "" || event.KeyFingerprint == string([]byte("customer-42")) {
		t.Fatalf("event key was not redacted: %#v", event)
	}
	if event.Winner == nil || event.Winner.NodeID != "east" {
		t.Fatalf("event winner = %#v", event.Winner)
	}

	page, err := log.ReadAfter(0, 8)
	if err != nil {
		t.Fatalf("ReadAfter() error = %v", err)
	}
	if len(page.Events) != 1 || page.Events[0].Sequence != event.Sequence || page.NextSequence != event.Sequence {
		t.Fatalf("page = %#v", page)
	}
	if _, err := log.Record(ConflictEventInput{
		Space:    "orders",
		Key:      []byte("customer-43"),
		Left:     left,
		Right:    right,
		Decision: ConflictEventDecisionRightWins,
		Winner:   &right,
	}); err != nil {
		t.Fatalf("second Record() error = %v", err)
	}
	second, err := log.ReadAfter(event.Sequence, 8)
	if err != nil {
		t.Fatalf("second ReadAfter() error = %v", err)
	}
	if len(second.Events) != 1 || second.Events[0].KeyFingerprint == event.KeyFingerprint {
		t.Fatalf("second page = %#v", second)
	}
}

func TestConflictEventLogBoundsWaitsAndRestores(t *testing.T) {
	path := filepath.Join(t.TempDir(), "conflicts.hce")
	options := ConflictEventLogOptions{Capacity: 2, Secret: []byte("conflict-event-test-secret")}
	log, err := OpenConflictEventLog(path, options)
	if err != nil {
		t.Fatalf("OpenConflictEventLog() error = %v", err)
	}
	left := ConflictVersion{Timestamp: 10, NodeID: "west", Sequence: 1}
	right := ConflictVersion{Timestamp: 11, NodeID: "east", Sequence: 2}
	record := func(key string) {
		t.Helper()
		if _, err := log.Record(ConflictEventInput{
			Space:    "orders",
			Key:      []byte(key),
			Left:     left,
			Right:    right,
			Decision: ConflictEventDecisionRightWins,
			Winner:   &right,
		}); err != nil {
			t.Fatalf("Record(%q) error = %v", key, err)
		}
	}
	record("one")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := log.WaitAfter(ctx, 1, 8); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitAfter() error = %v, want deadline", err)
	}
	waitResult := make(chan struct {
		page ConflictEventPage
		err  error
	}, 1)
	go func() {
		page, err := log.WaitAfter(context.Background(), 1, 8)
		waitResult <- struct {
			page ConflictEventPage
			err  error
		}{page: page, err: err}
	}()
	record("two")
	select {
	case result := <-waitResult:
		if result.err != nil || len(result.page.Events) != 1 || result.page.Events[0].Sequence != 2 {
			t.Fatalf("WaitAfter() result = %#v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitAfter() did not wake after Record()")
	}
	record("three")
	if _, err := log.ReadAfter(0, 8); !errors.Is(err, ErrConflictEventHistoryGap) {
		t.Fatalf("ReadAfter() gap error = %v, want %v", err, ErrConflictEventHistoryGap)
	}

	restored, err := OpenConflictEventLog(path, options)
	if err != nil {
		t.Fatalf("restore OpenConflictEventLog() error = %v", err)
	}
	page, err := restored.ReadAfter(1, 8)
	if err != nil {
		t.Fatalf("restored ReadAfter() error = %v", err)
	}
	if len(page.Events) != 2 || page.Events[0].Sequence != 2 || page.Events[1].Sequence != 3 {
		t.Fatalf("restored page = %#v", page)
	}
	if _, err := restored.Record(ConflictEventInput{
		Space:    "orders",
		Key:      []byte("bad"),
		Left:     left,
		Right:    right,
		Decision: ConflictEventDecisionRejected,
		Winner:   &right,
	}); !errors.Is(err, ErrConflictEventInvalid) {
		t.Fatalf("invalid rejected event error = %v, want %v", err, ErrConflictEventInvalid)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if _, err := OpenConflictEventLog(path, options); !errors.Is(err, ErrConflictEventFileInvalid) {
		t.Fatalf("corrupt OpenConflictEventLog() error = %v, want %v", err, ErrConflictEventFileInvalid)
	}
}

func TestConflictEventLogRejectsInvalidInput(t *testing.T) {
	if _, err := NewConflictEventLog(ConflictEventLogOptions{Secret: []byte("short")}); !errors.Is(err, ErrConflictEventSecretInvalid) {
		t.Fatalf("short secret error = %v, want %v", err, ErrConflictEventSecretInvalid)
	}
	log, err := NewConflictEventLog(ConflictEventLogOptions{Secret: []byte("conflict-event-test-secret")})
	if err != nil {
		t.Fatalf("NewConflictEventLog() error = %v", err)
	}
	left := ConflictVersion{Timestamp: 1, NodeID: "left", Sequence: 1}
	right := ConflictVersion{Timestamp: 2, NodeID: "right", Sequence: 1}
	if _, err := log.Record(ConflictEventInput{
		Space:    "orders",
		Key:      []byte("key"),
		Left:     left,
		Right:    right,
		Decision: ConflictEventDecisionLeftWins,
		Winner:   &right,
	}); !errors.Is(err, ErrConflictEventInvalid) {
		t.Fatalf("wrong winner error = %v, want %v", err, ErrConflictEventInvalid)
	}
	if _, err := log.ReadAfter(0, 0); !errors.Is(err, ErrConflictEventLimitInvalid) {
		t.Fatalf("invalid limit error = %v, want %v", err, ErrConflictEventLimitInvalid)
	}
	closed := make(chan error, 1)
	go func() {
		_, waitErr := log.WaitAfter(context.Background(), 0, 1)
		closed <- waitErr
	}()
	if err := log.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case waitErr := <-closed:
		if !errors.Is(waitErr, ErrConflictEventLogClosed) {
			t.Fatalf("closed WaitAfter() error = %v, want %v", waitErr, ErrConflictEventLogClosed)
		}
	case <-time.After(time.Second):
		t.Fatal("closed WaitAfter() did not wake")
	}
}
