#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' \
  -bench '^(BenchmarkM220MaterializedViewIndexLifecycle|BenchmarkM223MaterializedViewHydrationStatus)$' \
  -benchmem -count=5
