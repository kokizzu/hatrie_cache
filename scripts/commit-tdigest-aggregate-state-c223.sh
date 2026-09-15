#!/usr/bin/env bash
set -euo pipefail

unexpected=0
while IFS= read -r path; do
  case "$path" in
    Makefile|ADOPTED_QUERY_ENGINE_IDEAS.md|BENCHMARK.md|INSPIRATION_ROUND2.md|hat/hatDataStructure/partial_aggregate_envelope.go|hat/hatDataStructure/tdigest_aggregate_state.go|hat/hatDataStructure/tdigest_aggregate_state_test.go|hat/hatDataStructure/tdigest_aggregate_state_benchmark_test.go|scripts/run-tdigest-aggregate-state-c223.sh|scripts/format-tdigest-aggregate-state-c223.sh|scripts/check-tdigest-aggregate-state-c223.sh|scripts/stage-tdigest-aggregate-state-c223.sh|scripts/commit-tdigest-aggregate-state-c223.sh|scripts/push-tdigest-aggregate-state-c223.sh)
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
  printf '%s\n' 'no staged tdigest aggregate state changes' >&2
  exit 1
fi
git diff --cached --check --
git commit -m 'Add compact TDigest aggregate state'
