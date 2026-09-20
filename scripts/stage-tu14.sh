#!/usr/bin/env bash
set -euo pipefail
git add \
  hat/hatTopology/tu14_vshard.go \
  hat/hatTopology/tu14_vshard_test.go \
  hat/hatTopology/tu14_vshard_benchmark_test.go \
  TU14_VSHARD_BUCKET_MIGRATION.md \
  PRODUCT_IDEA_GAPS.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  README.md \
  BENCHMARK.md \
  Makefile \
  scripts/format-tu14.sh \
  scripts/test-tu14-red.sh \
  scripts/test-tu14-package.sh \
  scripts/test-tu14-all.sh \
  scripts/benchmark-tu14.sh \
  scripts/race-tu14.sh \
  scripts/vet-tu14.sh \
  scripts/review-tu14.sh \
  scripts/stage-tu14.sh \
  scripts/commit-tu14.sh \
  scripts/push-tu14.sh
