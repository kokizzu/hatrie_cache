#!/usr/bin/env bash
set -euo pipefail
git diff --check
git diff --cached --check
git diff --stat -- \
  hat/hatTopology/tu14_vshard.go \
  hat/hatTopology/tu14_vshard_test.go \
  hat/hatTopology/tu14_vshard_benchmark_test.go \
  TU14_VSHARD_BUCKET_MIGRATION.md \
  PRODUCT_IDEA_GAPS.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  README.md \
  BENCHMARK.md
git diff --cached --stat -- \
  hat/hatTopology/tu14_vshard.go \
  hat/hatTopology/tu14_vshard_test.go \
  hat/hatTopology/tu14_vshard_benchmark_test.go \
  TU14_VSHARD_BUCKET_MIGRATION.md \
  PRODUCT_IDEA_GAPS.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  README.md \
  BENCHMARK.md
git status --short
