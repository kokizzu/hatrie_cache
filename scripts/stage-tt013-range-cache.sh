#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ENGINE_IDEAS.md \
  Makefile \
  TT013_RANGE_TUPLE_CACHE.md \
  hat/hatDataStructure/range_tuple_cache.go \
  hat/hatDataStructure/tt013_range_tuple_cache_test.go \
  hat/hatDataStructure/tt013_range_tuple_cache_baseline_benchmark_test.go \
  hat/hatDataStructure/tt013_range_tuple_cache_benchmark_test.go \
  scripts/benchmark-tt013-range-cache.sh \
  scripts/benchmark-tt013-range-cache-baseline.sh \
  scripts/commit-tt013-range-cache.sh \
  scripts/format-tt013-range-cache.sh \
  scripts/push-tt013-range-cache.sh \
  scripts/stage-tt013-range-cache.sh \
  scripts/test-tt013-range-cache.sh \
  scripts/verify-tt013-range-cache.sh
