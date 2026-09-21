#!/usr/bin/env bash
set -euo pipefail

git diff --check -- \
  Makefile README.md ENGINE_IDEAS.md BENCHMARK.md \
  MZ046_FRONTIER_CANCELLATION.md \
  hat/hatPipeline/mz046_frontier_cancellation.go \
  hat/hatPipeline/mz046_frontier_cancellation_test.go \
  hat/hatPipeline/mz046_frontier_cancellation_benchmark_test.go \
  hat/hatPipeline/mz046_frontier_cancellation_baseline_benchmark_test.go \
  scripts/format-mz046-frontier-cancellation.sh \
  scripts/run-mz046-frontier-cancellation-benchmark.sh \
  scripts/test-mz046-frontier-cancellation.sh \
  scripts/race-mz046-frontier-cancellation.sh \
  scripts/verify-mz046-frontier-cancellation.sh \
  scripts/review-mz046-frontier-cancellation.sh \
  scripts/stage-mz046-frontier-cancellation.sh \
  scripts/commit-mz046-frontier-cancellation.sh \
  scripts/push-mz046-frontier-cancellation.sh
git status --short
git diff --stat -- \
  Makefile README.md ENGINE_IDEAS.md BENCHMARK.md \
  MZ046_FRONTIER_CANCELLATION.md \
  hat/hatPipeline/mz046_frontier_cancellation.go \
  hat/hatPipeline/mz046_frontier_cancellation_test.go \
  hat/hatPipeline/mz046_frontier_cancellation_benchmark_test.go \
  hat/hatPipeline/mz046_frontier_cancellation_baseline_benchmark_test.go \
  scripts
