package hatPipeline_test

import (
	"context"
	"testing"
	"time"

	"hatrie_cache/hat/hatPipeline"
)

func TestAsyncInsertDeduplicatorReusesExpiredIDWithoutFullSweep(t *testing.T) {
	now := time.Unix(500, 0)
	deduplicator, err := hatPipeline.NewAsyncInsertDeduplicator(hatPipeline.AsyncInsertDeduplicatorOptions{
		Capacity: 2,
		TTL:      time.Minute,
		Now:      func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewAsyncInsertDeduplicator() error = %v", err)
	}
	if _, err := deduplicator.Accept(context.Background(), "orders", "same", []byte("one")); err != nil {
		t.Fatalf("first Accept() error = %v", err)
	}
	now = now.Add(2 * time.Minute)
	decision, err := deduplicator.Accept(context.Background(), "orders", "same", []byte("two"))
	if err != nil || decision != hatPipeline.AsyncInsertAccepted {
		t.Fatalf("expired ID reuse = %v/%v, want accepted/nil", decision, err)
	}
	if stats := deduplicator.Stats(); stats.Entries != 1 || stats.Expired != 1 {
		t.Fatalf("expiry stats = %+v, want one live and one expired", stats)
	}
}
