package hatriecache

import (
	"sync"
	"time"
)

type RateLimiter struct {
	mu      sync.Mutex
	limit   int
	window  time.Duration
	now     func() time.Time
	clients map[string]rateLimitClient
}

type rateLimitClient struct {
	windowStart time.Time
	count       int
}

func NewRateLimiter(limit int, window time.Duration) *RateLimiter {
	if limit <= 0 || window <= 0 {
		return nil
	}
	return &RateLimiter{
		limit:   limit,
		window:  window,
		now:     time.Now,
		clients: make(map[string]rateLimitClient),
	}
}

func (limiter *RateLimiter) Allow(key string) bool {
	if limiter == nil || limiter.limit <= 0 {
		return true
	}
	if key == "" {
		key = "global"
	}
	now := limiter.now()
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	client := limiter.clients[key]
	if client.windowStart.IsZero() || now.Sub(client.windowStart) >= limiter.window {
		client = rateLimitClient{windowStart: now}
	}
	if client.count >= limiter.limit {
		limiter.clients[key] = client
		return false
	}
	client.count++
	limiter.clients[key] = client
	if len(limiter.clients) > 4096 {
		limiter.pruneLocked(now)
	}
	return true
}

func (limiter *RateLimiter) pruneLocked(now time.Time) {
	for key, client := range limiter.clients {
		if now.Sub(client.windowStart) >= limiter.window {
			delete(limiter.clients, key)
		}
	}
}
