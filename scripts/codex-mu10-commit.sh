#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  Makefile \
  MU010_AUTOMATIC_COMPACTION.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  hat/hatSql/differential_temporal_join.go \
  hat/hatSql/differential_temporal_join_compaction_scheduler.go \
  hat/hatSql/m_u10_compaction_scheduler_benchmark_test.go \
  hat/hatSql/m_u10_compaction_scheduler_test.go \
  scripts/codex-mu10-benchmark.sh \
  scripts/codex-mu10-commit.sh \
  scripts/codex-mu10-compact-benchmark.sh \
  scripts/codex-mu10-format.sh \
  scripts/codex-mu10-package.sh \
  scripts/codex-mu10-push.sh \
  scripts/codex-mu10-race.sh \
  scripts/codex-mu10-test.sh \
  scripts/codex-mu10-vet.sh
git commit -m 'feat(sql): add opt-in automatic compaction scheduler [skip ci]'
