#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLCompiledQueryCacheConcurrentMiss$' -benchmem -count=5
