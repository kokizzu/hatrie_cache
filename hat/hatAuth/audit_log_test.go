package hatAuth

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestAuditLogValidationAndRedaction(t *testing.T) {
	log, err := NewAuditLog(4)
	if err != nil {
		t.Fatalf("NewAuditLog() error = %v", err)
	}
	if _, err := log.Append(AuditRecord{Outcome: AuditOutcomeAllowed}); !errors.Is(err, ErrAuditCommandRequired) {
		t.Fatalf("missing command error = %v, want %v", err, ErrAuditCommandRequired)
	}
	if _, err := log.Append(AuditRecord{Command: "GET\n", Outcome: AuditOutcomeAllowed}); !errors.Is(err, ErrAuditFieldInvalid) {
		t.Fatalf("control character error = %v, want %v", err, ErrAuditFieldInvalid)
	}
	if _, err := log.Append(AuditRecord{Command: "SELECT *", Outcome: AuditOutcomeAllowed}); !errors.Is(err, ErrAuditFieldInvalid) {
		t.Fatalf("payload command error = %v, want %v", err, ErrAuditFieldInvalid)
	}
	if _, err := log.Append(AuditRecord{Command: "GET", Outcome: AuditOutcomeInvalid}); !errors.Is(err, ErrAuditOutcomeInvalid) {
		t.Fatalf("invalid outcome error = %v, want %v", err, ErrAuditOutcomeInvalid)
	}
	event, err := log.Append(AuditRecord{
		Principal: "Bearer secret-token",
		Command:   "SETSTR",
		Namespace: "tenant-eu",
		Outcome:   AuditOutcomeDenied,
		Metadata: []AuditMetadata{
			{Key: "Authorization", Value: "Bearer secret-token"},
			{Key: "reason", Value: "policy"},
		},
	})
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if event.Sequence != 1 || event.At.IsZero() || len(event.Metadata) != 2 {
		t.Fatalf("unexpected event = %#v", event)
	}
	if event.Metadata[0].Value != AuditRedactedValue || event.Metadata[1].Value != "policy" {
		t.Fatalf("metadata redaction = %#v", event.Metadata)
	}
	if event.Principal != AuditRedactedValue {
		t.Fatalf("bearer principal was not redacted: %q", event.Principal)
	}
	if event.Metadata[0].Key != "authorization" {
		t.Fatalf("metadata key normalization = %q", event.Metadata[0].Key)
	}
}

func TestAuditLogBoundedSequenceAndSince(t *testing.T) {
	now := time.Date(2026, time.October, 3, 1, 2, 3, 0, time.UTC)
	log, err := NewAuditLogWithOptions(AuditLogOptions{Capacity: 2, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("NewAuditLogWithOptions() error = %v", err)
	}
	for _, command := range []string{"GET", "SETSTR", "DEL"} {
		if _, err := log.Append(AuditRecord{Command: command, Outcome: AuditOutcomeAllowed}); err != nil {
			t.Fatalf("Append(%q) error = %v", command, err)
		}
	}
	stats := log.Stats()
	if stats.Capacity != 2 || stats.Entries != 2 || stats.FirstSequence != 2 || stats.LastSequence != 3 || stats.Dropped != 1 {
		t.Fatalf("unexpected stats = %#v", stats)
	}
	events := log.Snapshot()
	if len(events) != 2 || events[0].Sequence != 2 || events[1].Sequence != 3 || !events[0].At.Equal(now) {
		t.Fatalf("snapshot = %#v", events)
	}
	replayed, truncated := log.Since(0, nil)
	if !truncated || len(replayed) != 2 || replayed[0].Sequence != 2 {
		t.Fatalf("Since(0) = %#v, truncated=%v", replayed, truncated)
	}
	replayed, truncated = log.Since(2, replayed[:0])
	if truncated || len(replayed) != 1 || replayed[0].Sequence != 3 {
		t.Fatalf("Since(2) = %#v, truncated=%v", replayed, truncated)
	}
}

func TestAuditLogCopiesMetadataAndRejectsOversizeInput(t *testing.T) {
	log, err := NewAuditLog(2)
	if err != nil {
		t.Fatalf("NewAuditLog() error = %v", err)
	}
	metadata := []AuditMetadata{{Key: "region", Value: "eu"}}
	if _, err := log.Append(AuditRecord{Command: "GET", Outcome: AuditOutcomeAllowed, Metadata: metadata}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	metadata[0].Value = "mutated"
	snapshot := log.Snapshot()
	if snapshot[0].Metadata[0].Value != "eu" {
		t.Fatalf("stored metadata changed through input alias: %#v", snapshot)
	}
	snapshot[0].Metadata[0].Value = "snapshot-mutated"
	if log.Snapshot()[0].Metadata[0].Value != "eu" {
		t.Fatal("stored metadata changed through snapshot alias")
	}
	if _, err := log.Append(AuditRecord{Command: "GET", Outcome: AuditOutcomeAllowed, Metadata: make([]AuditMetadata, MaxAuditMetadataFields+1)}); !errors.Is(err, ErrAuditMetadataLimit) {
		t.Fatalf("oversize metadata error = %v, want %v", err, ErrAuditMetadataLimit)
	}
}

func TestAuditLogConcurrentAppendIsBounded(t *testing.T) {
	log, err := NewAuditLog(64)
	if err != nil {
		t.Fatalf("NewAuditLog() error = %v", err)
	}
	const writers = 8
	const recordsPerWriter = 64
	var wait sync.WaitGroup
	wait.Add(writers)
	for writer := 0; writer < writers; writer++ {
		go func() {
			defer wait.Done()
			for record := 0; record < recordsPerWriter; record++ {
				if _, err := log.Append(AuditRecord{Command: "GET", Outcome: AuditOutcomeAllowed}); err != nil {
					t.Errorf("Append() error = %v", err)
					return
				}
			}
		}()
	}
	wait.Wait()
	stats := log.Stats()
	if stats.Entries != 64 || stats.LastSequence != writers*recordsPerWriter || stats.Dropped != writers*recordsPerWriter-64 {
		t.Fatalf("unexpected concurrent stats = %#v", stats)
	}
}
