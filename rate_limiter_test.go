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
