#!/usr/bin/env bash
set -euo pipefail

git diff --check -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  C247_UINT64_DELTA_CODEC.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  scripts/format-c247-delta-codec.sh \
  hat/hatDataStructure/gorilla_float64_codec.go \
  hat/hatDataStructure/gorilla_float64_codec_test.go \
  hat/hatDataStructure/gorilla_float64_codec_benchmark_test.go \
  scripts/benchmark-c247-gorilla-codec.sh \
  scripts/race-c247-gorilla-codec.sh \
  scripts/review-c247-gorilla-codec.sh \
  scripts/stage-c247-gorilla-codec.sh \
  scripts/test-c247-gorilla-codec.sh

git diff --cached --check --

git status --short -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  C247_UINT64_DELTA_CODEC.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  scripts/format-c247-delta-codec.sh \
  hat/hatDataStructure/gorilla_float64_codec.go \
  hat/hatDataStructure/gorilla_float64_codec_test.go \
  hat/hatDataStructure/gorilla_float64_codec_benchmark_test.go \
  scripts/benchmark-c247-gorilla-codec.sh \
  scripts/race-c247-gorilla-codec.sh \
  scripts/review-c247-gorilla-codec.sh \
  scripts/stage-c247-gorilla-codec.sh \
  scripts/test-c247-gorilla-codec.sh
