#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  C244_LOCAL_CACHE_REUSE.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  hat/hatMerkle/c244_part_manifest_benchmark_test.go \
  hat/hatMerkle/c244_part_manifest_test.go \
  hat/hatMerkle/part_manifest.go \
  scripts/benchmark-c244-isolated.sh \
  scripts/commit-c244-local-cache-reuse.sh \
  scripts/format-c244-isolated.sh \
  scripts/push-c244-local-cache-reuse.sh \
  scripts/stage-c244-local-cache-reuse.sh \
  scripts/test-c244-isolated.sh
