package hatRate

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

func TestRateLimiterDefersShardMapsUntilFirstUse(t *testing.T) {
	limiter := NewRateLimiter(1, time.Second)
	if len(limiter.shards) != rateLimiterShardCount {
		t.Fatalf("shards len = %d, want %d", len(limiter.shards), rateLimiterShardCount)
	}
	for idx := range limiter.shards {
		if limiter.shards[idx].clients != nil {
			t.Fatalf("shard %d clients map is initialized before use", idx)
		}
	}
	if !limiter.Allow("client") {
		t.Fatal("Allow(client) = false, want true")
	}
	initialized := rateLimiterShardIndex("client")
	for idx := range limiter.shards {
		wantInitialized := idx == initialized
		if gotInitialized := limiter.shards[idx].clients != nil; gotInitialized != wantInitialized {
			t.Fatalf("shard %d initialized = %v, want %v", idx, gotInitialized, wantInitialized)
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

func BenchmarkRateLimiterAllowSameClientAlternating(b *testing.B) {
	const allows = 1 << 18
	lazy := NewRateLimiter(1<<30, time.Second)
	eager := newEagerRateLimiterControl(1<<30, time.Second)
	now := time.Unix(100, 0)
	lazy.now = func() time.Time { return now }
	eager.now = lazy.now
	if !lazy.Allow("client") || !allowRateLimiterEagerControl(eager, "client") {
		b.Fatal("initial Allow(client) = false, want true")
	}

	var lazyDuration, eagerDuration time.Duration
	for iteration := 0; iteration < b.N; iteration++ {
		lazyFirst := iteration&1 != 0
		for pass := 0; pass < 2; pass++ {
			started := time.Now()
			if lazyFirst == (pass == 0) {
				for idx := 0; idx < allows; idx++ {
					if !lazy.Allow("client") {
						b.Fatal("lazy Allow(client) = false, want true")
					}
				}
				lazyDuration += time.Since(started)
			} else {
				for idx := 0; idx < allows; idx++ {
					if !allowRateLimiterEagerControl(eager, "client") {
						b.Fatal("eager Allow(client) = false, want true")
					}
				}
				eagerDuration += time.Since(started)
			}
		}
	}
	operations := float64(b.N * allows)
	b.ReportMetric(float64(eagerDuration.Nanoseconds())/operations, "eager-ns/allow")
	b.ReportMetric(float64(lazyDuration.Nanoseconds())/operations, "lazy-ns/allow")
}

func newEagerRateLimiterControl(limit int, window time.Duration) *RateLimiter {
	limiter := &RateLimiter{limit: limit, window: window, now: time.Now}
	for idx := range limiter.shards {
		limiter.shards[idx].clients = make(map[string]rateLimitClient)
	}
	return limiter
}

func allowRateLimiterEagerControl(limiter *RateLimiter, key string) bool {
	if limiter == nil || limiter.limit <= 0 {
		return true
	}
	if key == "" {
		key = "global"
	}
	now := limiter.now()
	shard := limiter.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	client, ok := shard.clients[key]
	if !ok || client.lastSeen.IsZero() {
		client = rateLimitClient{lastSeen: now, tokens: float64(limiter.limit)}
	} else {
		elapsed := now.Sub(client.lastSeen)
		if elapsed < 0 {
			elapsed = 0
		}
		client.tokens += float64(limiter.limit) * float64(elapsed) / float64(limiter.window)
		if maxTokens := float64(limiter.limit); client.tokens > maxTokens {
			client.tokens = maxTokens
		}
		client.lastSeen = now
	}
	if client.tokens < 1 {
		shard.clients[key] = client
		return false
	}
	client.tokens--
	shard.clients[key] = client
	if len(shard.clients) > rateLimiterMaxClientsPerShard {
		shard.pruneLocked(now, limiter.window)
	}
	return true
}

var benchmarkRateLimiterSink *RateLimiter

func BenchmarkRateLimiterConstruction(b *testing.B) {
	b.ReportAllocs()
	for idx := 0; idx < b.N; idx++ {
		benchmarkRateLimiterSink = NewRateLimiter(100, time.Second)
	}
}

func BenchmarkRateLimiterFirstClientLifecycle(b *testing.B) {
	now := time.Unix(100, 0)
	nowFunc := func() time.Time { return now }
	for _, lazy := range []bool{false, true} {
		name := "EagerControl"
		if lazy {
			name = "Lazy"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for idx := 0; idx < b.N; idx++ {
				var limiter *RateLimiter
				if lazy {
					limiter = NewRateLimiter(100, time.Second)
				} else {
					limiter = newEagerRateLimiterControl(100, time.Second)
				}
				limiter.now = nowFunc
				if !limiter.Allow("client") {
					b.Fatal("Allow(client) = false, want true")
				}
				benchmarkRateLimiterSink = limiter
			}
		})
	}
}

func BenchmarkRateLimiterAllShardsLifecycle(b *testing.B) {
	keys := make([]string, rateLimiterShardCount)
	for candidate, found := 0, 0; found < len(keys); candidate++ {
		key := "client-" + strconv.Itoa(candidate)
		shard := rateLimiterShardIndex(key)
		if keys[shard] == "" {
			keys[shard] = key
			found++
		}
	}
	now := time.Unix(100, 0)
	nowFunc := func() time.Time { return now }
	for _, lazy := range []bool{false, true} {
		name := "EagerControl"
		if lazy {
			name = "Lazy"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				var limiter *RateLimiter
				if lazy {
					limiter = NewRateLimiter(100, time.Second)
				} else {
					limiter = newEagerRateLimiterControl(100, time.Second)
				}
				limiter.now = nowFunc
				for _, key := range keys {
					if !limiter.Allow(key) {
						b.Fatalf("Allow(%q) = false, want true", key)
					}
				}
				benchmarkRateLimiterSink = limiter
			}
		})
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
