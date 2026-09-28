package hatRate

import (
	"testing"
	"time"
)

func TestRateLimiterAllowNIsAtomic(t *testing.T) {
	now := time.Unix(100, 0)
	limiter := NewRateLimiter(4, time.Second)
	limiter.now = func() time.Time { return now }

	if !limiter.AllowN("client", 3) {
		t.Fatal("AllowN(client, 3) = false, want true")
	}
	if limiter.AllowN("client", 2) {
		t.Fatal("AllowN(client, 2) = true, want false without partial consumption")
	}
	if !limiter.Allow("client") {
		t.Fatal("Allow(client) = false, want the one remaining token")
	}
	if limiter.Allow("client") {
		t.Fatal("Allow(client) = true, want false after the batch and single request")
	}
}

func TestRateLimiterAllowNRefillsAndHandlesBounds(t *testing.T) {
	now := time.Unix(100, 0)
	limiter := NewRateLimiter(4, time.Second)
	limiter.now = func() time.Time { return now }

	if limiter.AllowN("client", 5) {
		t.Fatal("AllowN(client, 5) = true, want false when request exceeds burst")
	}
	if !limiter.AllowN("client", 0) {
		t.Fatal("AllowN(client, 0) = false, want true")
	}
	if !limiter.AllowN("client", 2) {
		t.Fatal("AllowN(client, 2) = false, want true")
	}
	now = now.Add(500 * time.Millisecond)
	if !limiter.AllowN("client", 2) {
		t.Fatal("AllowN(client, 2) after refill = false, want true")
	}
}
