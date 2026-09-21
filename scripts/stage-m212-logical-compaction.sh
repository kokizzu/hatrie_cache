#!/usr/bin/env bash
set -euo pipefail
git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M212_LOGICAL_COMPACTION.md \
  Makefile \
  hat/hatSql/typed_table_mvcc.go \
  hat/hatSql/m212_logical_compaction_test.go \
  hat/hatSql/m212_logical_compaction_baseline_benchmark_test.go \
  hat/hatSql/m212_logical_compaction_benchmark_test.go \
  scripts/benchmark-m212-logical-compaction-baseline.sh \
  scripts/benchmark-m212-logical-compaction.sh \
  scripts/commit-m212-logical-compaction.sh \
  scripts/format-m212-logical-compaction.sh \
  scripts/push-m212-logical-compaction.sh \
  scripts/race-m212-logical-compaction.sh \
  scripts/stage-m212-logical-compaction.sh \
  scripts/test-m212-logical-compaction-package.sh \
  scripts/test-m212-logical-compaction.sh \
  scripts/verify-m212-logical-compaction.sh \
  scripts/vet-m212-logical-compaction.sh
