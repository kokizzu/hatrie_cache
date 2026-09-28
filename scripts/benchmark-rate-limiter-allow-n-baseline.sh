#!/usr/bin/env bash
set -euo pipefail

baseline_file="hat/hatRate/rate_allow_n_baseline_temp_test.go"
trap 'rm -f "$baseline_file"' EXIT
cat > "$baseline_file" <<'EOF'
package hatRate

import (
	"testing"
	"time"
)

var rateLimiterAllowNBaselineSink *RateLimiter

func BenchmarkRateLimiterRepeatedAllowBaseline(b *testing.B) {
	limiter := NewRateLimiter(1<<30, time.Second)
	limiter.now = func() time.Time { return time.Unix(100, 0) }
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		for request := 0; request < 8; request++ {
			if !limiter.Allow("client") {
				b.Fatal("Allow(client) = false, want true")
			}
		}
	}
	rateLimiterAllowNBaselineSink = limiter
}
EOF
go test ./hat/hatRate/rate_limiter.go "$baseline_file" -run '^$' -bench '^BenchmarkRateLimiterRepeatedAllowBaseline$' -benchmem -count=5
