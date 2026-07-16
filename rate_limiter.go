package hatriecache

import (
	"sync"
	"time"
)

const rateLimiterMaxClients = 4096

type RateLimiter struct {
	mu      sync.Mutex
	limit   int
	window  time.Duration
	now     func() time.Time
	clients map[string]rateLimitClient
}

type rateLimitClient struct {
	lastSeen time.Time
	tokens   float64
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

func (limiter *RateLimiter) Limit() int {
	if limiter == nil {
		return 0
	}
	return limiter.limit
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
	client, ok := limiter.clients[key]
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
		limiter.clients[key] = client
		return false
	}
	client.tokens--
	limiter.clients[key] = client
	if len(limiter.clients) > rateLimiterMaxClients {
		limiter.pruneLocked(now)
	}
	return true
}

func (limiter *RateLimiter) pruneLocked(now time.Time) {
	for key, client := range limiter.clients {
		if now.Sub(client.lastSeen) >= limiter.window {
			delete(limiter.clients, key)
		}
	}
	for len(limiter.clients) > rateLimiterMaxClients {
		oldestKey := ""
		var oldest time.Time
		for key, client := range limiter.clients {
			if oldestKey == "" || client.lastSeen.Before(oldest) {
				oldestKey = key
				oldest = client.lastSeen
			}
		}
		if oldestKey == "" {
			return
		}
		delete(limiter.clients, oldestKey)
	}
}
