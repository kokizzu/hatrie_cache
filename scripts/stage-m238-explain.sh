#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M238_EXPLAIN_PUSHDOWN.md \
  Makefile \
  hat/hatSql/m238_explain_pushdown_test.go \
  hat/hatSql/mu012_arrangement_explain.go \
  hat/hatSql/query.go \
  scripts/benchmark-m238-explain.sh \
  scripts/commit-m238-explain.sh \
  scripts/push-m238-explain.sh \
  scripts/stage-m238-explain.sh \
  scripts/test-m238-explain.sh

git diff --cached --check
git diff --cached --stat
git status --short
