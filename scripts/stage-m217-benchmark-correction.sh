#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  M217_MATERIALIZED_POINT_LOOKUP.md \
  scripts/stage-m217-benchmark-correction.sh \
  scripts/commit-m217-benchmark-correction.sh \
  scripts/push-m217-benchmark-correction.sh
