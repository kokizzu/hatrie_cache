#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkC211JoinOrder(Baseline|Statistics)$' -benchmem -count=5
