#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkC213CompilePlan(Baseline|Cached)$' -benchmem -count=5 -cpu=1
