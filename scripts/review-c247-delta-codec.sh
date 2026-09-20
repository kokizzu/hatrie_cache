#!/usr/bin/env bash
set -euo pipefail

git diff --check -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  C247_UINT64_DELTA_CODEC.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  hat/hatDataStructure/delta_uint64_codec.go \
  hat/hatDataStructure/delta_uint64_codec_test.go \
  hat/hatDataStructure/delta_uint64_codec_benchmark_test.go \
  scripts/benchmark-c247-delta-codec.sh \
  scripts/commit-c247-delta-codec.sh \
  scripts/format-c247-delta-codec.sh \
  scripts/push-c247-delta-codec.sh \
  scripts/race-c247-delta-codec.sh \
  scripts/review-c247-delta-codec.sh \
  scripts/stage-c247-delta-codec.sh \
  scripts/test-c247-delta-codec-package.sh \
  scripts/test-c247-delta-codec.sh \
  scripts/vet-c247-delta-codec.sh

git status --short -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  C247_UINT64_DELTA_CODEC.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  hat/hatDataStructure/delta_uint64_codec.go \
  hat/hatDataStructure/delta_uint64_codec_test.go \
  hat/hatDataStructure/delta_uint64_codec_benchmark_test.go \
  scripts/benchmark-c247-delta-codec.sh \
  scripts/commit-c247-delta-codec.sh \
  scripts/format-c247-delta-codec.sh \
  scripts/push-c247-delta-codec.sh \
  scripts/race-c247-delta-codec.sh \
  scripts/review-c247-delta-codec.sh \
  scripts/stage-c247-delta-codec.sh \
  scripts/test-c247-delta-codec-package.sh \
  scripts/test-c247-delta-codec.sh \
  scripts/vet-c247-delta-codec.sh
