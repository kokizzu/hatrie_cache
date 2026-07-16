package hatriecache

import (
	"testing"
	"time"
)

func TestRateLimiterAllowsConfiguredWindow(t *testing.T) {
	now := time.Unix(100, 0)
	limiter := NewRateLimiter(2, time.Second)
	limiter.now = func() time.Time { return now }

	if !limiter.Allow("client") || !limiter.Allow("client") {
		t.Fatal("first two rate limit attempts should be allowed")
	}
	if limiter.Allow("client") {
		t.Fatal("third rate limit attempt in same window should be rejected")
	}
	now = now.Add(time.Second)
	if !limiter.Allow("client") {
		t.Fatal("rate limit should reset in next window")
	}
}

func TestRateLimiterRefillsTokensWithinWindow(t *testing.T) {
	now := time.Unix(100, 0)
	limiter := NewRateLimiter(2, time.Second)
	limiter.now = func() time.Time { return now }

	if !limiter.Allow("client") || !limiter.Allow("client") {
		t.Fatal("initial burst should consume available tokens")
	}
	if limiter.Allow("client") {
		t.Fatal("third immediate attempt should be rejected")
	}
	now = now.Add(500 * time.Millisecond)
	if !limiter.Allow("client") {
		t.Fatal("half-window refill should allow one request")
	}
	if limiter.Allow("client") {
		t.Fatal("refilled token should be consumed")
	}
	now = now.Add(500 * time.Millisecond)
	if !limiter.Allow("client") {
		t.Fatal("next half-window refill should allow another request")
	}
}

func TestRateLimiterBoundsClientState(t *testing.T) {
	now := time.Unix(100, 0)
	limiter := NewRateLimiter(1, time.Hour)
	limiter.now = func() time.Time { return now }

	for idx := 0; idx < rateLimiterMaxClients+64; idx++ {
		if !limiter.Allow(string(rune('a' + idx))) {
			t.Fatalf("unique client %d was rejected", idx)
		}
		now = now.Add(time.Millisecond)
	}
	if len(limiter.clients) > rateLimiterMaxClients {
		t.Fatalf("client state len = %d, want <= %d", len(limiter.clients), rateLimiterMaxClients)
	}
}

func BenchmarkRateLimiterAllowSameClient(b *testing.B) {
	now := time.Unix(100, 0)
	limiter := NewRateLimiter(b.N+1, time.Second)
	limiter.now = func() time.Time { return now }

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		if !limiter.Allow("client") {
			b.Fatal("Allow(client) = false, want true")
		}
	}
}
