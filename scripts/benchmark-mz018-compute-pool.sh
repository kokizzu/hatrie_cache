#!/usr/bin/env bash
set -euo pipefail

mkdir -p build/benchmarks
go test ./hat/hatSql -run '^$' -bench '(^BenchmarkSQLQueryManager$|^BenchmarkMZ018SQLQueryManagerComputePool|^BenchmarkMZ018SQLQueryManagerLegacyConcurrent)' -benchmem -count=5 | tee build/benchmarks/mz018-compute-pool-after.txt
printf '%s\n' '--- captured benchmark rows ---'
awk '/^Benchmark(SQLQueryManager|MZ018SQLQueryManagerComputePool)/ { print }' build/benchmarks/mz018-compute-pool-after.txt
