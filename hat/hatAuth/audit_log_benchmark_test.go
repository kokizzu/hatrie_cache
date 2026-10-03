package hatAuth

import "testing"

type round78BaselineAuditRing struct {
	entries []AuditEvent
	next    uint64
}

func newRound78BaselineAuditRing(capacity int) *round78BaselineAuditRing {
	return &round78BaselineAuditRing{entries: make([]AuditEvent, capacity)}
}

func (ring *round78BaselineAuditRing) append(event AuditEvent) {
	index := ring.next % uint64(len(ring.entries))
	event.Sequence = ring.next + 1
	ring.entries[index] = event
	ring.next++
}

func BenchmarkRound78BaselineAuditAppend(b *testing.B) {
	ring := newRound78BaselineAuditRing(1024)
	event := AuditEvent{Command: "GET", Outcome: AuditOutcomeAllowed, Principal: "operator", Namespace: "tenant-eu"}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		ring.append(event)
	}
}

func BenchmarkRound78AuditAppend(b *testing.B) {
	log, err := NewAuditLog(1024)
	if err != nil {
		b.Fatal(err)
	}
	record := AuditRecord{Command: "GET", Outcome: AuditOutcomeAllowed, Principal: "operator", Namespace: "tenant-eu"}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := log.Append(record); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRound78AuditAppendRedactedMetadata(b *testing.B) {
	log, err := NewAuditLog(1024)
	if err != nil {
		b.Fatal(err)
	}
	record := AuditRecord{
		Command:  "GET",
		Outcome:  AuditOutcomeAllowed,
		Metadata: []AuditMetadata{{Key: "authorization", Value: "Bearer secret-token"}},
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := log.Append(record); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRound78AuditSnapshot(b *testing.B) {
	log, err := NewAuditLog(1024)
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 1024; index++ {
		if _, err := log.Append(AuditRecord{Command: "GET", Outcome: AuditOutcomeAllowed}); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if len(log.Snapshot()) != 1024 {
			b.Fatal("unexpected snapshot length")
		}
	}
}
