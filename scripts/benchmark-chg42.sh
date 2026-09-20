#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCHG42SQLQueryMemoryTracking$' -benchmem -benchtime=10000x -count=3
