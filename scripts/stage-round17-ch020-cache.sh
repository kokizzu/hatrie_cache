#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  CH020_ZERO_COPY_PARTS.md \
  ENGINE_IDEAS.md \
  Makefile \
  README.md \
  hat/hatStorage/ch020_zero_copy_benchmark_test.go \
  hat/hatStorage/ch020_zero_copy_test.go \
  hat/hatStorage/remote_part_cache.go \
  scripts/commit-round17-ch020-cache.sh \
  scripts/push-round17-ch020-cache.sh \
  scripts/run-ch020-zero-copy-checks.sh \
  scripts/stage-round17-ch020-cache.sh
git diff --cached --check
