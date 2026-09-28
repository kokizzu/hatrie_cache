#!/usr/bin/env bash
set -euo pipefail

source_file="hat/hatRate/rate_limiter_single_baseline_temp.go"
test_file="hat/hatRate/rate_limiter_single_baseline_temp_test.go"
trap 'rm -f "$source_file" "$test_file"' EXIT
git show HEAD:hat/hatRate/rate_limiter.go > "$source_file"
cat > "$test_file" <<'EOF'
package hatRate

import (
	"testing"
	"time"
)

var rateLimiterSingleBaselineSink *RateLimiter

func BenchmarkRateLimiterAllowSameClientBaseline(b *testing.B) {
	now := time.Unix(100, 0)
	limiter := NewRateLimiter(b.N+1, time.Second)
	limiter.now = func() time.Time { return now }
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if !limiter.Allow("client") {
			b.Fatal("Allow(client) = false, want true")
		}
	}
	rateLimiterSingleBaselineSink = limiter
}
EOF
go test "./$source_file" "./$test_file" -run '^$' -bench '^BenchmarkRateLimiterAllowSameClientBaseline$' -benchmem -count=5
