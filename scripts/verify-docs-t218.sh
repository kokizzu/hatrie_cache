#!/usr/bin/env bash
set -euo pipefail

for path in \
  T218_MULTI_PART_TREE_INDEX.md \
  README.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  INSPIRATION_ROUND2.md \
  BENCHMARK.md; do
  if [[ ! -f "$path" ]]; then
    printf 'missing T218 documentation file: %s\n' "$path" >&2
    exit 1
  fi
done

rg -q 'T218_MULTI_PART_TREE_INDEX\.md' README.md
rg -q 'Multi-part TREE-style ordered runs' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q 'T218 Multi-part TREE indexes' INSPIRATION_ROUND2.md
rg -q '## T218: Multi-Part TREE Index' BENCHMARK.md
rg -q 'T218_MULTI_PART_TREE_INDEX\.md' BENCHMARK.md
