#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH032QueryProfilerRecord$' -benchmem -count="${COUNT:-5}" -benchtime="${BENCHTIME:-200ms}"
