#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^$' -bench '^BenchmarkM065yMutableRangeNthValue' -benchmem -benchtime=200ms -count=5
