#!/usr/bin/env bash
set -euo pipefail

scripts=(
  scripts/run-topk-merge-c222.sh
  scripts/format-topk-merge-c222.sh
  scripts/check-topk-merge-c222.sh
  scripts/stage-topk-merge-c222.sh
  scripts/commit-topk-merge-c222.sh
  scripts/push-topk-merge-c222.sh
)
for script in "${scripts[@]}"; do
  bash -n "$script"
done

git diff --check -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  hat/hatCache/top_k_aggregate.go \
  hat/hatCache/top_k_merge_test.go \
  hat/hatCache/top_k_merge_benchmark_test.go \
  hat/hatDataStructure/partial_aggregate_envelope.go \
  scripts/run-topk-merge-c222.sh \
  scripts/format-topk-merge-c222.sh \
  scripts/check-topk-merge-c222.sh \
  scripts/stage-topk-merge-c222.sh \
  scripts/commit-topk-merge-c222.sh \
  scripts/push-topk-merge-c222.sh

git diff --cached --check -- \
  Makefile \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  hat/hatCache/top_k_aggregate.go \
  hat/hatCache/top_k_merge_test.go \
  hat/hatCache/top_k_merge_benchmark_test.go \
  hat/hatDataStructure/partial_aggregate_envelope.go \
  scripts/run-topk-merge-c222.sh \
  scripts/format-topk-merge-c222.sh \
  scripts/check-topk-merge-c222.sh \
  scripts/stage-topk-merge-c222.sh \
  scripts/commit-topk-merge-c222.sh \
  scripts/push-topk-merge-c222.sh

printf '%s\n' 'top-k merge checks passed'
