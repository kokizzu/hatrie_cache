package hatAudit

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

func TestAuditLoggerSamplingRetainsFailuresAndExportsAcceptedEvents(t *testing.T) {
	var output bytes.Buffer
	var exported []AuditEvent
	logger, err := NewAuditLoggerWithOptions(&output, AuditLoggerOptions{
		SuccessSampleRate: 0.5,
		Sinks: []AuditSink{AuditSinkFunc(func(event AuditEvent) error {
			exported = append(exported, event)
			return nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 1000; index++ {
		if err := logger.Log(AuditEvent{Action: "success", OK: true}); err != nil {
			t.Fatal(err)
		}
	}
	if err := logger.Log(AuditEvent{Action: "denied", OK: false}); err != nil {
		t.Fatal(err)
	}

	if len(exported) < 400 || len(exported) > 600 {
		t.Fatalf("exported events = %d, want roughly half of 1000 successes plus one failure", len(exported))
	}
	failures := 0
	for _, event := range exported {
		if !event.OK {
			failures++
		}
	}
	if failures != 1 {
		t.Fatalf("exported failures = %d, want 1", failures)
	}
	if logger.SampledSuccessEvents() < 400 || logger.SampledSuccessEvents() > 600 {
		t.Fatalf("sampled successes = %d, want roughly half of 1000", logger.SampledSuccessEvents())
	}
	if got := len(auditEventsFromJSONL(t, output.String())); got != len(exported) {
		t.Fatalf("JSONL events = %d, exported events = %d", got, len(exported))
	}
}

func TestAuditLoggerDefaultRemainsLossless(t *testing.T) {
	var output bytes.Buffer
	logger := NewAuditLogger(&output)
	for index := 0; index < 256; index++ {
		if err := logger.Log(AuditEvent{Action: "success", OK: true}); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(auditEventsFromJSONL(t, output.String())); got != 256 {
		t.Fatalf("default JSONL events = %d, want 256", got)
	}
	if got := logger.SampledSuccessEvents(); got != 0 {
		t.Fatalf("default sampled successes = %d, want 0", got)
	}
}

func TestAuditLoggerSinkErrorsAreReturned(t *testing.T) {
	wantErr := errors.New("sink unavailable")
	logger, err := NewAuditLoggerWithOptions(nil, AuditLoggerOptions{
		Sinks: []AuditSink{AuditSinkFunc(func(AuditEvent) error { return wantErr })},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := logger.Log(AuditEvent{Action: "export", OK: true}); !errors.Is(err, wantErr) {
		t.Fatalf("Log() error = %v, want %v", err, wantErr)
	}
}

func TestAuditLoggerRejectsInvalidSamplingRates(t *testing.T) {
	for _, rate := range []float64{-0.1, 1.1} {
		if _, err := NewAuditLoggerWithOptions(nil, AuditLoggerOptions{SuccessSampleRate: rate}); err == nil {
			t.Fatalf("rate %v accepted", rate)
		}
	}
}

func TestAuditLoggerFullRecentWindowDoesNotAllocatePerEvent(t *testing.T) {
	logger := NewAuditLogger(nil)
	for index := 0; index < MaxRecentAuditEvents; index++ {
		if err := logger.Log(AuditEvent{Time: "2026-01-01T00:00:00Z", Action: "warmup", OK: true}); err != nil {
			t.Fatal(err)
		}
	}
	allocs := testing.AllocsPerRun(100, func() {
		if err := logger.Log(AuditEvent{Time: "2026-01-01T00:00:00Z", Action: "steady", OK: true}); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("full recent window allocations = %v, want 0", allocs)
	}
}

func TestAuditLoggerRecentRingPreservesNewestQueryOrder(t *testing.T) {
	logger := NewAuditLogger(nil)
	for index := 0; index < MaxRecentAuditEvents+3; index++ {
		if err := logger.Log(AuditEvent{
			Time:   fmt.Sprintf("2026-01-01T00:00:%03dZ", index),
			Action: fmt.Sprintf("event-%03d", index),
		}); err != nil {
			t.Fatal(err)
		}
	}
	recent := logger.Recent(3)
	if got := []string{recent[0].Action, recent[1].Action, recent[2].Action}; fmt.Sprint(got) != "[event-130 event-129 event-128]" {
		t.Fatalf("Recent(3) = %v, want newest events", got)
	}
	query, err := logger.Query(Query{Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{query[0].Action, query[1].Action, query[2].Action}; fmt.Sprint(got) != "[event-130 event-129 event-128]" {
		t.Fatalf("Query(limit=3) = %v, want newest events", got)
	}
}
