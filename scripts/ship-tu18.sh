#!/usr/bin/env bash
set -euo pipefail

branch=codex/inspiration-tu18-volatile-engine-20261006
git add \
  BENCHMARK.md \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  TU018_EXPLICIT_VOLATILE_ENGINE.md \
  hat/hatCache/volatile_engine.go \
  hat/hatCache/volatile_engine_after_benchmark_test.go \
  hat/hatCache/volatile_engine_benchmark_test.go \
  hat/hatCache/volatile_engine_test.go \
  scripts/benchmark-tu18.sh \
  scripts/clean-tu18.sh \
  scripts/format-tu18.sh \
  scripts/test-tu18.sh \
  scripts/verify-tu18.sh \
  scripts/ship-tu18.sh
git diff --cached --check
git diff --cached --stat
git commit -m 'feat: add explicit volatile cache engine [skip ci]'
git push --set-upstream origin "$branch"
