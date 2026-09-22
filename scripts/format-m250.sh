#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/temporal_join_frontier_alignment.go \
  hat/hatSql/m250_temporal_join_alignment_test.go \
  hat/hatSql/m250_temporal_join_alignment_benchmark_test.go
