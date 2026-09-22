#!/usr/bin/env bash
set -euo pipefail

git diff --check
git add -- \
  BENCHMARK.md \
  C236_EXPLAIN_SKIP_DECISIONS.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  hat/hatSql/c236_explain_skip_decisions_test.go \
  hat/hatSql/ch056_like_program_test.go \
  hat/hatSql/materialized.go \
  hat/hatSql/model.go \
  hat/hatSql/mz050_plan_snapshot.go \
  hat/hatSql/query.go \
  hat/hatSql/result_cache.go \
  scripts/benchmark-c236-before-after.sh \
  scripts/format-c236-explain.sh \
  scripts/race-c236-explain.sh \
  scripts/test-c236-explain.sh \
  scripts/vet-c236-explain.sh \
  scripts/stage-c236-explain.sh \
  scripts/commit-c236-explain.sh \
  scripts/push-c236-explain.sh
git diff --cached --check
git status --short
