#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CH015_FILESYSTEM_CACHE_ADMISSION.md \
  ENGINE_IDEAS.md \
  Makefile \
  README.md \
  hat/hatStorage/remote_part_cache.go \
  hat/hatStorage/remote_part_cache_c245_benchmark_test.go \
  hat/hatStorage/remote_part_cache_c245_test.go \
  scripts/benchmark-c245.sh \
  scripts/commit-c245-filesystem-cache-admission.sh \
  scripts/format-c245.sh \
  scripts/push-c245-filesystem-cache-admission.sh \
  scripts/race-c245.sh \
  scripts/stage-c245-filesystem-cache-admission.sh \
  scripts/test-c245.sh
