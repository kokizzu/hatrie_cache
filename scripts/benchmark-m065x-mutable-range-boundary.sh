#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^$' -bench '^BenchmarkM065xMutableRangeBoundary' -benchmem -benchtime=200ms -count=5
