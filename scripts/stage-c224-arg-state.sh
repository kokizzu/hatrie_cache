#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  C224_ARGMAX_ARGMIN_STATE.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  hat/hatDataStructure/arg_extreme_aggregate_state.go \
  hat/hatDataStructure/c224_arg_extreme_state_test.go \
  hat/hatDataStructure/c224_arg_state_baseline_test.go \
  hat/hatDataStructure/partial_aggregate_envelope.go \
  scripts/benchmark-c224-arg-state.sh \
  scripts/commit-c224-arg-state.sh \
  scripts/format-c224-arg-state.sh \
  scripts/push-c224-arg-state.sh \
  scripts/race-c224-arg-state.sh \
  scripts/stage-c224-arg-state.sh \
  scripts/test-c224-arg-state.sh \
  scripts/verify-c224-arg-state.sh \
  scripts/vet-c224-arg-state.sh

git diff --cached --check
git diff --cached --stat
