#!/usr/bin/env bash
set -euo pipefail

scripts=(
  scripts/run-tdigest-aggregate-state-c223.sh
  scripts/format-tdigest-aggregate-state-c223.sh
  scripts/check-tdigest-aggregate-state-c223.sh
  scripts/stage-tdigest-aggregate-state-c223.sh
  scripts/commit-tdigest-aggregate-state-c223.sh
  scripts/push-tdigest-aggregate-state-c223.sh
)
for script in "${scripts[@]}"; do
  bash -n "$script"
done

git diff --check -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  hat/hatDataStructure/partial_aggregate_envelope.go \
  hat/hatDataStructure/tdigest_aggregate_state.go \
  hat/hatDataStructure/tdigest_aggregate_state_test.go \
  hat/hatDataStructure/tdigest_aggregate_state_benchmark_test.go \
  scripts/run-tdigest-aggregate-state-c223.sh \
  scripts/format-tdigest-aggregate-state-c223.sh \
  scripts/check-tdigest-aggregate-state-c223.sh \
  scripts/stage-tdigest-aggregate-state-c223.sh \
  scripts/commit-tdigest-aggregate-state-c223.sh \
  scripts/push-tdigest-aggregate-state-c223.sh

git diff --cached --check -- \
  Makefile \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  hat/hatDataStructure/partial_aggregate_envelope.go \
  hat/hatDataStructure/tdigest_aggregate_state.go \
  hat/hatDataStructure/tdigest_aggregate_state_test.go \
  hat/hatDataStructure/tdigest_aggregate_state_benchmark_test.go \
  scripts/run-tdigest-aggregate-state-c223.sh \
  scripts/format-tdigest-aggregate-state-c223.sh \
  scripts/check-tdigest-aggregate-state-c223.sh \
  scripts/stage-tdigest-aggregate-state-c223.sh \
  scripts/commit-tdigest-aggregate-state-c223.sh \
  scripts/push-tdigest-aggregate-state-c223.sh

printf '%s\n' 'tdigest aggregate state checks passed'
