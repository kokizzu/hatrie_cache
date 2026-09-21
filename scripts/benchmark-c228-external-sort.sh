#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkC228ExternalSortStableRuns$' -benchmem -count=5
