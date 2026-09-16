#!/usr/bin/env bash
set -euo pipefail
for file in CHU12_BACKGROUND_INDEX_REBUILD_QUEUE.md README.md PRODUCT_IDEA_GAPS.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md; do
  test -f "$file"
done
rg -q 'CHU12_BACKGROUND_INDEX_REBUILD_QUEUE.md' README.md
rg -q 'CH-U12' PRODUCT_IDEA_GAPS.md
rg -q 'Background prioritized skip-index rebuild queue' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q 'benchmark-chu12-c274' BENCHMARK.md
