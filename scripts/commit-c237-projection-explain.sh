#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  C237_PROJECTION_EXPLAIN.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  hat/hatSql/c237_estimated_io_cost_test.go \
  hat/hatSql/explain_dataflow.go \
  hat/hatSql/explain_pipeline.go \
  hat/hatSql/model.go \
  hat/hatSql/mz044_costed_explain.go \
  hat/hatSql/mz044_costed_explain_test.go \
  hat/hatSql/query.go \
  hat/hatSql/result_cache.go \
  scripts/benchmark-c237-projection-explain.sh \
  scripts/commit-c237-projection-explain.sh \
  scripts/format-c237-projection-explain.sh \
  scripts/push-c237-projection-explain.sh \
  scripts/race-c237-projection-explain.sh \
  scripts/test-c237-projection-explain.sh \
  scripts/verify-c237-projection-explain.sh \
  scripts/vet-c237-projection-explain.sh

git diff --cached --check
git commit -m 'feat: add estimated projection IO explain cost [skip ci]'
