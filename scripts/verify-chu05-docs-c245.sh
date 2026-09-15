#!/usr/bin/env bash
set -euo pipefail

for file in README.md BENCHMARK.md PRODUCT_IDEA_GAPS.md ADOPTED_QUERY_ENGINE_IDEAS.md CHU05_EXTERNAL_WINDOW_STREAM.md; do
	test -s "$file"
done

rg -q 'CHU05_EXTERNAL_WINDOW_STREAM\.md' README.md PRODUCT_IDEA_GAPS.md ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q 'ch-u05-external-window-streaming' README.md ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q '^## CH-U05 External Window Streaming$' BENCHMARK.md CHU05_EXTERNAL_WINDOW_STREAM.md
rg -q 'make benchmark-chu05-c245' BENCHMARK.md CHU05_EXTERNAL_WINDOW_STREAM.md

printf '%s\n' 'CH-U05 documentation references verified.'
