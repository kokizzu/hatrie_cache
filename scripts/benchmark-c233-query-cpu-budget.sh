#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkC233QueryCPUTimeBudget$' -benchmem -count=5
