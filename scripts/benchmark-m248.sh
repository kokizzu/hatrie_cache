#!/usr/bin/env bash
set -euo pipefail

bench_regex='^(BenchmarkSQLResultCacheVersionedMiss|BenchmarkMaintainedResultCacheVersionedMiss|BenchmarkResultCacheConcurrentDuplicateMiss|BenchmarkMaintainedResultCacheConcurrentDuplicateMiss|BenchmarkResultCacheConcurrentSerializedMiss|BenchmarkMaintainedResultCacheConcurrentSerializedMiss)$'
if [[ "${M248_BENCH_MODE:-all}" == "serialized" ]]; then
	bench_regex='^(BenchmarkResultCacheConcurrentSerializedMiss|BenchmarkMaintainedResultCacheConcurrentSerializedMiss)$'
fi
go test ./hat/hatSql -run '^$' -bench "$bench_regex" -benchmem -count=5
