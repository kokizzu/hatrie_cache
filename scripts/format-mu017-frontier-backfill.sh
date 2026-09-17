#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/incremental_projection.go \
  hat/hatSql/mu017_frontier_backfill.go \
  hat/hatSql/mu017_frontier_backfill_test.go \
  hat/hatSql/mu017_frontier_backfill_baseline_benchmark_test.go \
  hat/hatSql/mu017_frontier_backfill_benchmark_test.go
git diff --check
