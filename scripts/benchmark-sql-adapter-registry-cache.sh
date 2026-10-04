#!/usr/bin/env bash
set -euo pipefail

count="${BENCH_COUNT:-5}"

go test ./hat/hatStorage \
  -run '^$' \
  -bench 'BenchmarkSQLAdapterRegistryExecuteResolverOnly(NoCache)?$' \
  -benchmem \
  -count "$count"
