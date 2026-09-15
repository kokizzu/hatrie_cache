#!/usr/bin/env bash
set -euo pipefail

tmp_makefile="$(mktemp)"
trap 'rm -f "$tmp_makefile"' EXIT

git show HEAD:Makefile > "$tmp_makefile"
printf '%s\n' \
  '' \
  '.PHONY: test-topk-merge-c222 format-topk-merge-c222 test-topk-merge-package-c222 race-topk-merge-c222 vet-topk-merge-c222 benchmark-topk-merge-c222 check-topk-merge-c222 stage-topk-merge-c222 commit-topk-merge-c222 push-topk-merge-c222' \
  '' \
  'test-topk-merge-c222:' \
  $'\tbash ./scripts/run-topk-merge-c222.sh test' \
  '' \
  'format-topk-merge-c222:' \
  $'\tbash ./scripts/format-topk-merge-c222.sh' \
  '' \
  'test-topk-merge-package-c222:' \
  $'\tbash ./scripts/run-topk-merge-c222.sh package' \
  '' \
  'race-topk-merge-c222:' \
  $'\tbash ./scripts/run-topk-merge-c222.sh race' \
  '' \
  'vet-topk-merge-c222:' \
  $'\tbash ./scripts/run-topk-merge-c222.sh vet' \
  '' \
  'benchmark-topk-merge-c222:' \
  $'\tbash ./scripts/run-topk-merge-c222.sh benchmark' \
  '' \
  'check-topk-merge-c222:' \
  $'\tbash ./scripts/check-topk-merge-c222.sh' \
  '' \
  'stage-topk-merge-c222:' \
  $'\tbash ./scripts/stage-topk-merge-c222.sh' \
  '' \
  'commit-topk-merge-c222:' \
  $'\tbash ./scripts/commit-topk-merge-c222.sh' \
  '' \
  'push-topk-merge-c222:' \
  $'\tbash ./scripts/push-topk-merge-c222.sh' \
  >> "$tmp_makefile"

blob="$(git hash-object -w "$tmp_makefile")"
git update-index --add --cacheinfo 100644 "$blob" Makefile
git add -- \
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

printf '%s\n' 'staged top-k merge feature paths'
