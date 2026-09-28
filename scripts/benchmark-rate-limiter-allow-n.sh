#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatRate -run '^$' -bench '^BenchmarkRateLimiter(RepeatedAllowBaseline|AllowNBatch)$' -benchmem -count=5
