#!/usr/bin/env bash
set -euo pipefail

go test -run '^$' -bench '^BenchmarkC211JoinOrder(Statistics|Baseline)$' -benchmem -count=5 ./hat/hatSql
