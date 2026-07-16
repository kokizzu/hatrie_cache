package hatriecache

import (
	"sync"
	"time"
)

const (
	rateLimiterMaxClients = 4096
	rateLimiterShardCount = 64
)

type RateLimiter struct {
	limit  int
	window time.Duration
	now    func() time.Time
	shards [rateLimiterShardCount]rateLimiterShard
}

type rateLimiterShard struct {
	mu      sync.Mutex
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
	limiter := &RateLimiter{
		limit:  limit,
		window: window,
		now:    time.Now,
	}
	for idx := range limiter.shards {
		limiter.shards[idx].clients = make(map[string]rateLimitClient)
	}
	return limiter
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

func (limiter *RateLimiter) shardFor(key string) *rateLimiterShard {
	return &limiter.shards[rateLimiterShardIndex(key)]
}

const rateLimiterMaxClientsPerShard = rateLimiterMaxClients / rateLimiterShardCount

func rateLimiterShardIndex(key string) int {
	hash := uint64(1469598103934665603)
	for idx := 0; idx < len(key); idx++ {
		hash ^= uint64(key[idx])
		hash *= 1099511628211
	}
	return int(hash & uint64(rateLimiterShardCount-1))
}

func (shard *rateLimiterShard) pruneLocked(now time.Time, window time.Duration) {
	for key, client := range shard.clients {
		if now.Sub(client.lastSeen) >= window {
			delete(shard.clients, key)
		}
	}
	for len(shard.clients) > rateLimiterMaxClientsPerShard {
		oldestKey := ""
		var oldest time.Time
		for key, client := range shard.clients {
			if oldestKey == "" || client.lastSeen.Before(oldest) {
				oldestKey = key
				oldest = client.lastSeen
			}
		}
		if oldestKey == "" {
			return
		}
		delete(shard.clients, oldestKey)
	}
}
