#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatRate -run '^$' -bench '^BenchmarkRateLimiterAllowSameClient$' -benchmem -count=5
