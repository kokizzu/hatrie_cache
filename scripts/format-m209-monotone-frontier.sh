#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/m209_monotone_timestamp.go \
  hat/hatDataStructure/m209_monotone_timestamp_test.go \
  hat/hatDataStructure/m209_monotone_timestamp_baseline_benchmark_test.go \
  hat/hatDataStructure/m209_monotone_timestamp_benchmark_test.go \
  hat/hatReplication/m209_monotone_frontier_test.go \
  hat/hatReplication/m209_monotone_frontier_benchmark_test.go \
  hat/hatReplication/m209_monotone_frontier_baseline_benchmark_test.go \
  hat/hatSql/m209_monotone_frontier_test.go \
  hat/hatSql/m209_monotone_frontier_benchmark_test.go
