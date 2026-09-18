#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  MZ046_SCHEMA_MIGRATION_BARRIER.md \
  Makefile \
  README.md \
  hat/hatPipeline/mz046_schema_migration_barrier.go \
  hat/hatPipeline/mz046_schema_migration_barrier_benchmark_test.go \
  hat/hatPipeline/mz046_schema_migration_barrier_test.go \
  scripts/benchmark-mz046-schema-migration-barrier.sh \
  scripts/commit-mz046.sh \
  scripts/format-mz046-schema-migration-barrier.sh \
  scripts/push-mz046.sh \
  scripts/race-mz046-schema-migration-barrier.sh \
  scripts/review-mz046.sh \
  scripts/stage-mz046.sh \
  scripts/test-mz046-package.sh \
  scripts/test-mz046-schema-migration-barrier.sh \
  scripts/vet-mz046-schema-migration-barrier.sh

git diff --cached --check
git diff --cached --stat
git status --short
