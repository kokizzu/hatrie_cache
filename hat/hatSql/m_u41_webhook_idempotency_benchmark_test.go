//go:build mu41

package hatSql

import (
	"strconv"
	"testing"
	"time"
)

func BenchmarkMU41WebhookEventAcceptDuplicate(b *testing.B) {
	now := time.Unix(1_700_000_000, 0).UTC()
	deduplicator, err := NewWebhookEventDeduplicator(WebhookEventDeduplicatorOptions{Capacity: 1, TTL: time.Hour})
	if err != nil {
		b.Fatal(err)
	}
	payload := []byte(`{"event":"payment.created","amount":10,"currency":"USD"}`)
	if _, err := deduplicator.Accept("payments", "event-1", payload, now); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if decision, err := deduplicator.Accept("payments", "event-1", payload, now); err != nil || decision != WebhookEventDuplicate {
			b.Fatalf("duplicate Accept = %v/%v", decision, err)
		}
	}
}

func BenchmarkMU41WebhookEventSnapshot(b *testing.B) {
	now := time.Unix(1_700_000_000, 0).UTC()
	deduplicator, err := NewWebhookEventDeduplicator(WebhookEventDeduplicatorOptions{Capacity: 1024, TTL: time.Hour})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 1024; index++ {
		if _, err := deduplicator.Accept("payments", "event-"+strconv.Itoa(index), []byte("payload"), now); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := deduplicator.Snapshot(); err != nil {
			b.Fatal(err)
		}
	}
}
