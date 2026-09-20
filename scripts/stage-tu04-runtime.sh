#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  TU04_BOUNDED_RUNTIME.md \
  hat/hatRuntime/sandbox.go \
  hat/hatRuntime/tu04_sandbox_benchmark_test.go \
  hat/hatRuntime/tu04_sandbox_test.go \
  scripts/audit-hatrie-tmp.sh \
  scripts/benchmark-tu04-runtime.sh \
  scripts/cleanup-hatrie-build-tmp.sh \
  scripts/commit-tu04-runtime.sh \
  scripts/format-tu04-runtime.sh \
  scripts/push-tu04-runtime.sh \
  scripts/race-tu04-runtime.sh \
  scripts/review-tu04-runtime.sh \
  scripts/stage-tu04-runtime.sh \
  scripts/test-tu04-runtime-package.sh \
  scripts/test-tu04-runtime.sh \
  scripts/vet-tu04-runtime.sh
