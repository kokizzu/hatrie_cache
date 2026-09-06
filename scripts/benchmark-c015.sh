#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPredicate -run '^$' -bench '^BenchmarkMatch(Int64|Int64SIMD)$' -benchmem -count=5
