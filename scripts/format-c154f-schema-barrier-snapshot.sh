#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatPipeline/c154f_schema_migration_barrier_snapshot.go \
  hat/hatPipeline/c154f_schema_migration_barrier_snapshot_test.go \
  hat/hatPipeline/c154f_schema_migration_barrier_snapshot_baseline_benchmark_test.go
