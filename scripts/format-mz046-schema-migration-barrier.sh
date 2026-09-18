#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatPipeline/mz046_schema_migration_barrier.go \
  hat/hatPipeline/mz046_schema_migration_barrier_benchmark_test.go \
  hat/hatPipeline/mz046_schema_migration_barrier_test.go
