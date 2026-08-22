package hatriecache

import (
	"time"

	"hatrie_cache/hat/hatRate"
)

// RateLimiter is the importable hatRate limiter retained under the root API
// for compatibility with existing cache integrations.
type RateLimiter = hatRate.RateLimiter

// NewRateLimiter constructs a bounded token-bucket rate limiter.
func NewRateLimiter(limit int, window time.Duration) *RateLimiter {
	return hatRate.NewRateLimiter(limit, window)
}
