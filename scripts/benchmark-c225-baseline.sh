#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkC225WindowAggregateBaseline$' -benchmem -count=5
