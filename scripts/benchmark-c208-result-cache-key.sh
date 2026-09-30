#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLResultCacheKeyFastpathPaired$' -benchmem -count=12
