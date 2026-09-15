#!/usr/bin/env bash
set -euo pipefail

unexpected=0
while IFS= read -r path; do
  case "$path" in
    Makefile|ADOPTED_QUERY_ENGINE_IDEAS.md|BENCHMARK.md|INSPIRATION_ROUND2.md|hat/hatCache/top_k_aggregate.go|hat/hatCache/top_k_merge_test.go|hat/hatCache/top_k_merge_benchmark_test.go|hat/hatDataStructure/partial_aggregate_envelope.go|scripts/run-topk-merge-c222.sh|scripts/format-topk-merge-c222.sh|scripts/check-topk-merge-c222.sh|scripts/stage-topk-merge-c222.sh|scripts/commit-topk-merge-c222.sh|scripts/push-topk-merge-c222.sh)
      ;;
    *)
      printf 'unexpected staged path: %s\n' "$path" >&2
      unexpected=1
      ;;
  esac
done < <(git diff --cached --name-only)
if (( unexpected )); then
  exit 1
fi
if git diff --cached --quiet --; then
  printf '%s\n' 'no staged top-k merge changes' >&2
  exit 1
fi
git diff --cached --check --
git commit -m 'Add mergeable approximate top-K state'
