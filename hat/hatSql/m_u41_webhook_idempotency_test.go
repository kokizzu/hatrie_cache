//go:build mu41

package hatSql

import (
	"bytes"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestMU41WebhookEventDeduplicatorAcceptsDuplicatesAndRejectsConflicts(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	deduplicator, err := NewWebhookEventDeduplicator(WebhookEventDeduplicatorOptions{
		Capacity: 2,
		TTL:      time.Hour,
	})
	if err != nil {
		t.Fatalf("NewWebhookEventDeduplicator: %v", err)
	}

	decision, err := deduplicator.Accept("payments", "event-1", []byte(`{"amount":10}`), now)
	if err != nil || decision != WebhookEventAccepted {
		t.Fatalf("first Accept = %v/%v, want accepted", decision, err)
	}
	decision, err = deduplicator.Accept("payments", "event-1", []byte(`{"amount":10}`), now.Add(time.Minute))
	if err != nil || decision != WebhookEventDuplicate {
		t.Fatalf("duplicate Accept = %v/%v, want duplicate", decision, err)
	}
	if _, err := deduplicator.Accept("payments", "event-1", []byte(`{"amount":11}`), now.Add(time.Minute)); !errors.Is(err, ErrWebhookEventConflict) {
		t.Fatalf("conflicting Accept error = %v, want conflict", err)
	}
	if _, err := deduplicator.Accept("payments", "event-2", []byte("second"), now); err != nil {
		t.Fatalf("second event: %v", err)
	}
	if _, err := deduplicator.Accept("payments", "event-3", []byte("third"), now); !errors.Is(err, ErrWebhookEventCapacity) {
		t.Fatalf("full Accept error = %v, want capacity", err)
	}
	if removed := deduplicator.Prune(now.Add(time.Hour)); removed != 2 {
		t.Fatalf("Prune removed %d, want 2", removed)
	}
	if decision, err := deduplicator.Accept("payments", "event-1", []byte(`{"amount":11}`), now.Add(time.Hour)); err != nil || decision != WebhookEventAccepted {
		t.Fatalf("expired Accept = %v/%v, want accepted", decision, err)
	}
}

func TestMU41WebhookEventDeduplicatorSnapshotIsDeterministicAndAtomic(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	options := WebhookEventDeduplicatorOptions{Capacity: 8, TTL: time.Hour}
	first, err := NewWebhookEventDeduplicator(options)
	if err != nil {
		t.Fatalf("NewWebhookEventDeduplicator first: %v", err)
	}
	second, err := NewWebhookEventDeduplicator(options)
	if err != nil {
		t.Fatalf("NewWebhookEventDeduplicator second: %v", err)
	}
	for _, event := range []struct {
		source string
		id     string
		body   string
	}{
		{source: "z-source", id: "z-id", body: "z"},
		{source: "a-source", id: "a-id", body: "a"},
	} {
		if _, err := first.Accept(event.source, event.id, []byte(event.body), now); err != nil {
			t.Fatalf("first Accept(%q): %v", event.id, err)
		}
	}
	snapshot, err := first.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if !bytes.HasPrefix(snapshot, []byte("HWE1")) {
		t.Fatalf("snapshot prefix = %q, want HWE1", snapshot[:mu41MinInt(len(snapshot), 4)])
	}
	if err := second.Restore(snapshot, now.Add(time.Minute)); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	restored, err := second.Snapshot()
	if err != nil {
		t.Fatalf("restored Snapshot: %v", err)
	}
	if !bytes.Equal(snapshot, restored) {
		t.Fatalf("restored snapshot differs from original")
	}

	corrupted := append([]byte(nil), snapshot...)
	corrupted[len(corrupted)-1] ^= 1
	if err := second.Restore(corrupted, now); !errors.Is(err, ErrWebhookEventSnapshot) {
		t.Fatalf("corrupt Restore error = %v, want snapshot error", err)
	}
	if decision, err := second.Accept("a-source", "a-id", []byte("a"), now); err != nil || decision != WebhookEventDuplicate {
		t.Fatalf("state after failed Restore = %v/%v, want duplicate", decision, err)
	}
}

func TestMU41WebhookEventDeduplicatorValidatesBoundsAndConcurrentReplay(t *testing.T) {
	invalidOptions := []WebhookEventDeduplicatorOptions{
		{Capacity: -1},
		{TTL: -time.Second},
		{MaxSourceBytes: -1},
		{MaxEventIDBytes: -1},
		{MaxPayloadBytes: -1},
		{MaxSnapshotBytes: -1},
	}
	for _, options := range invalidOptions {
		if _, err := NewWebhookEventDeduplicator(options); !errors.Is(err, ErrWebhookEventInvalid) {
			t.Fatalf("options %#v error = %v", options, err)
		}
	}

	now := time.Unix(1_700_000_000, 0).UTC()
	deduplicator, err := NewWebhookEventDeduplicator(WebhookEventDeduplicatorOptions{Capacity: 1, TTL: time.Hour})
	if err != nil {
		t.Fatalf("NewWebhookEventDeduplicator: %v", err)
	}
	for _, event := range [][2]string{{"", "id"}, {"source", ""}} {
		if _, err := deduplicator.Accept(event[0], event[1], []byte("body"), now); !errors.Is(err, ErrWebhookEventInvalid) {
			t.Fatalf("invalid event %#v error = %v", event, err)
		}
	}

	const workers = 16
	var group sync.WaitGroup
	decisions := make(chan WebhookEventDecision, workers)
	errorsSeen := make(chan error, workers)
	group.Add(workers)
	for index := 0; index < workers; index++ {
		go func() {
			defer group.Done()
			decision, err := deduplicator.Accept("source", "same-id", []byte("body"), now)
			decisions <- decision
			errorsSeen <- err
		}()
	}
	group.Wait()
	close(decisions)
	close(errorsSeen)
	accepted := 0
	duplicates := 0
	for decision := range decisions {
		switch decision {
		case WebhookEventAccepted:
			accepted++
		case WebhookEventDuplicate:
			duplicates++
		default:
			t.Fatalf("unexpected decision %v", decision)
		}
	}
	for err := range errorsSeen {
		if err != nil {
			t.Fatalf("concurrent Accept error = %v", err)
		}
	}
	if accepted != 1 || duplicates != workers-1 {
		t.Fatalf("concurrent decisions accepted=%d duplicates=%d", accepted, duplicates)
	}
}

func mu41MinInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}
