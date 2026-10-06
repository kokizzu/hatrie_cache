#!/usr/bin/env bash
set -euo pipefail

count=${BENCH_COUNT:-5}
go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLPreparedQueryCache(Hit|NormalizedAliasHit|VersionedHit)$' -benchmem -count="$count"
