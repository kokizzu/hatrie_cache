#!/usr/bin/env bash
set -euo pipefail

tmp_makefile="$(mktemp)"
trap 'rm -f "$tmp_makefile"' EXIT

git show HEAD:Makefile > "$tmp_makefile"
printf '%s\n' \
  '' \
  '.PHONY: test-tdigest-aggregate-state-c223 format-tdigest-aggregate-state-c223 test-tdigest-aggregate-state-package-c223 race-tdigest-aggregate-state-c223 vet-tdigest-aggregate-state-c223 benchmark-tdigest-aggregate-state-c223 check-tdigest-aggregate-state-c223 stage-tdigest-aggregate-state-c223 commit-tdigest-aggregate-state-c223 push-tdigest-aggregate-state-c223' \
  '' \
  'test-tdigest-aggregate-state-c223:' \
  $'\tbash ./scripts/run-tdigest-aggregate-state-c223.sh test' \
  '' \
  'format-tdigest-aggregate-state-c223:' \
  $'\tbash ./scripts/format-tdigest-aggregate-state-c223.sh' \
  '' \
  'test-tdigest-aggregate-state-package-c223:' \
  $'\tbash ./scripts/run-tdigest-aggregate-state-c223.sh package' \
  '' \
  'race-tdigest-aggregate-state-c223:' \
  $'\tbash ./scripts/run-tdigest-aggregate-state-c223.sh race' \
  '' \
  'vet-tdigest-aggregate-state-c223:' \
  $'\tbash ./scripts/run-tdigest-aggregate-state-c223.sh vet' \
  '' \
  'benchmark-tdigest-aggregate-state-c223:' \
  $'\tbash ./scripts/run-tdigest-aggregate-state-c223.sh benchmark' \
  '' \
  'check-tdigest-aggregate-state-c223:' \
  $'\tbash ./scripts/check-tdigest-aggregate-state-c223.sh' \
  '' \
  'stage-tdigest-aggregate-state-c223:' \
  $'\tbash ./scripts/stage-tdigest-aggregate-state-c223.sh' \
  '' \
  'commit-tdigest-aggregate-state-c223:' \
  $'\tbash ./scripts/commit-tdigest-aggregate-state-c223.sh' \
  '' \
  'push-tdigest-aggregate-state-c223:' \
  $'\tbash ./scripts/push-tdigest-aggregate-state-c223.sh' \
  >> "$tmp_makefile"

blob="$(git hash-object -w "$tmp_makefile")"
git update-index --add --cacheinfo 100644 "$blob" Makefile
git add -- \
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

printf '%s\n' 'staged tdigest aggregate state feature paths'
