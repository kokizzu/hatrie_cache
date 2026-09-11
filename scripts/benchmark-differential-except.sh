#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkExceptDifferentialRows/(BaselineComposition|Optimized)$' -benchmem -count=5
