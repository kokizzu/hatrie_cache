#!/usr/bin/env bash
set -euo pipefail

expected_paths=(
  Makefile
  ENGINE_IDEAS.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  TT024_TEXT_INDEX_CATALOG.md
  hat/hatSchema/tt024_text_index_catalog.go
  hat/hatSchema/tt024_text_index_catalog_test.go
  hat/hatSchema/tt024_text_index_catalog_benchmark_test.go
  scripts/test-tt024-text-index-catalog.sh
  scripts/benchmark-tt024-text-index-catalog.sh
  scripts/format-tt024-text-index-catalog.sh
  scripts/race-tt024-text-index-catalog.sh
  scripts/vet-tt024-text-index-catalog.sh
  scripts/stage-tt024-text-index-catalog.sh
  scripts/commit-tt024-text-index-catalog.sh
  scripts/push-tt024-text-index-catalog.sh
)

mapfile -t staged_paths < <(git diff --cached --name-only --)
if (( ${#staged_paths[@]} != ${#expected_paths[@]} )); then
  printf 'unexpected staged path count: expected %s, got %s\n' "${#expected_paths[@]}" "${#staged_paths[@]}" >&2
  printf 'Staged paths:\n' >&2
  printf '  %s\n' "${staged_paths[@]}" >&2
  exit 1
fi
for expected in "${expected_paths[@]}"; do
  found=0
  for staged in "${staged_paths[@]}"; do
    [[ "$staged" == "$expected" ]] && found=1
  done
  (( found == 1 )) || {
    printf 'expected staged path is missing: %s\n' "$expected" >&2
    exit 1
  }
done

git diff --cached --check
git commit -m 'feat: add external text index catalog hooks [skip ci]'
