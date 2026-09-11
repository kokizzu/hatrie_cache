#!/usr/bin/env bash
set -euo pipefail

mkdir -p build/benchmarks
go test ./hat/hatSql -run '^$' -bench '^(BenchmarkNamespaceQueryGateFastPath|BenchmarkNamespaceQueryQuotaDisabledPath|BenchmarkMZ019NamespaceGovernorLegacyConcurrent|BenchmarkMZ019NamespaceGovernorNamedPoolsConcurrent)$' -benchmem -count=5 | tee build/benchmarks/mz019-resource-pools-after.txt
printf '%s\n' '--- captured benchmark rows ---'
awk '/^Benchmark(NamespaceQueryGateFastPath|NamespaceQueryQuotaDisabledPath|MZ019NamespaceGovernorLegacyConcurrent|MZ019NamespaceGovernorNamedPoolsConcurrent)/ { print }' build/benchmarks/mz019-resource-pools-after.txt
