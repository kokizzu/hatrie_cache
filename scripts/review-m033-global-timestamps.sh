#!/usr/bin/env bash
set -euo pipefail

gofmt -d \
  hat/hatReplication/global_timestamp_oracle.go \
  hat/hatReplication/global_timestamp_oracle_test.go \
  hat/hatReplication/global_timestamp_oracle_benchmark_test.go
go test ./hat/hatReplication -run '^TestGlobalTimestampOracle' -count=1
go test -race ./hat/hatReplication -run '^TestGlobalTimestampOracle' -count=1
go vet ./hat/hatReplication
git diff --check
printf '%s\n' '--- M033b documentation references ---'
rg -n -A18 -B2 'M033b|Global Timestamp|global timestamp' GLOBAL_TIMESTAMP_ORACLE.md BENCHMARK.md README.md INSPIRATION.md CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
printf '%s\n' '--- worktree ---'
git status --short
