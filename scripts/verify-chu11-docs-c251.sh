#!/usr/bin/env bash
set -euo pipefail

test -s CHU11_AUTOMATIC_DATA_SKIPPING_INDEX_SELECTION.md
rg -F -q 'CHU11_AUTOMATIC_DATA_SKIPPING_INDEX_SELECTION.md' README.md
rg -F -q 'CHU11_AUTOMATIC_DATA_SKIPPING_INDEX_SELECTION.md' PRODUCT_IDEA_GAPS.md
rg -F -q 'CHU11_AUTOMATIC_DATA_SKIPPING_INDEX_SELECTION.md' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -F -q '## CH-U11 Automatic Data-Skipping-Index Selection' BENCHMARK.md
rg -F -q 'make benchmark-chu11-before-c251' BENCHMARK.md
rg -F -q 'make benchmark-chu11-c251' BENCHMARK.md
