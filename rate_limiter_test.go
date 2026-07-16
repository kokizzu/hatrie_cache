package hatriecache

import (
	"strconv"
	"sync/atomic"
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
	if got := rateLimiterClientCount(limiter); got > rateLimiterMaxClients {
		t.Fatalf("client state len = %d, want <= %d", got, rateLimiterMaxClients)
	}
}

func TestRateLimiterInitializesShards(t *testing.T) {
	limiter := NewRateLimiter(1, time.Second)
	if len(limiter.shards) != rateLimiterShardCount {
		t.Fatalf("shards len = %d, want %d", len(limiter.shards), rateLimiterShardCount)
	}
	for idx := range limiter.shards {
		if limiter.shards[idx].clients == nil {
			t.Fatalf("shard %d clients map is nil", idx)
		}
	}
}

func rateLimiterClientCount(limiter *RateLimiter) int {
	if limiter == nil {
		return 0
	}
	total := 0
	for idx := range limiter.shards {
		total += len(limiter.shards[idx].clients)
	}
	return total
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

func BenchmarkRateLimiterAllowParallelClients(b *testing.B) {
	keys := make([]string, 1024)
	for idx := range keys {
		keys[idx] = "client-" + strconv.Itoa(idx)
	}
	limiter := NewRateLimiter(b.N+1, time.Second)
	limiter.now = func() time.Time { return time.Unix(100, 0) }
	var next uint64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := keys[int(atomic.AddUint64(&next, 1))&(len(keys)-1)]
			if !limiter.Allow(key) {
				b.Fatal("Allow(client) = false, want true")
			}
		}
	})
}
