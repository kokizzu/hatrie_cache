package hatRate_test

import (
	"testing"
	"time"

	"hatrie_cache/hat/hatRate"
)

func TestNewRateLimiterIsUsableByImporters(t *testing.T) {
	limiter := hatRate.NewRateLimiter(1, time.Second)
	if limiter == nil {
		t.Fatal("NewRateLimiter() = nil")
	}
	if limiter.Limit() != 1 {
		t.Fatalf("Limit() = %d, want 1", limiter.Limit())
	}
	if !limiter.Allow("client") || limiter.Allow("client") {
		t.Fatal("Allow() did not enforce the configured limit")
	}
}
