package hatRate

import (
	"testing"
	"time"
)

var rateLimiterAllowNFastpathSink *RateLimiter

func BenchmarkRateLimiterAllowNBatch(b *testing.B) {
	limiter := NewRateLimiter(1<<30, time.Second)
	limiter.now = func() time.Time { return time.Unix(100, 0) }
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if !limiter.AllowN("client", 8) {
			b.Fatal("AllowN(client, 8) = false, want true")
		}
	}
	rateLimiterAllowNFastpathSink = limiter
}
