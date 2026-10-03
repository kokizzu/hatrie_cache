#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench 'BenchmarkSQLPreparedQueryCache(Hit|ExactLookupBaseline|Eviction)$' -benchmem -count=5
