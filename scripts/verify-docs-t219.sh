#!/usr/bin/env bash
set -euo pipefail

for path in \
  T219_PACKED_HASH_INDEX.md \
  README.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  INSPIRATION_ROUND2.md \
  BENCHMARK.md; do
  if [[ ! -f "$path" ]]; then
    printf 'missing T219 documentation file: %s\n' "$path" >&2
    exit 1
  fi
done

rg -q 'T219_PACKED_HASH_INDEX\.md' README.md
rg -q 'Packed HASH primary/exact-match table' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q 'T219 HASH indexes' INSPIRATION_ROUND2.md
rg -q '## T219: Packed HASH Index' BENCHMARK.md
rg -q 'T219_PACKED_HASH_INDEX\.md' BENCHMARK.md
