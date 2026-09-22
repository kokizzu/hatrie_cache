#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M237_LAZY_HYDRATION.md \
  Makefile \
  hat/hatSql/materialized.go \
  hat/hatSql/materialized_hydration.go \
  hat/hatSql/m237_lazy_hydration_test.go \
  scripts/test-m237-lazy-hydration.sh \
  scripts/stage-m237-lazy-hydration.sh \
  scripts/commit-m237-lazy-hydration.sh \
  scripts/push-m237-lazy-hydration.sh

git diff --cached --check
git diff --cached --stat
git status --short
