#!/bin/sh
set -eu

for path in CHU38_QUERY_LOG_SAMPLING.md BENCHMARK.md README.md QUERY_HISTORY.md PRODUCT_IDEA_GAPS.md INSPIRATION.md ADOPTED_QUERY_ENGINE_IDEAS.md; do
	if [ ! -f "$path" ]; then
		printf 'missing documentation file: %s\n' "$path" >&2
		exit 1
	fi
done
rg -q 'CH-U38 Sampled Query-Log Export' CHU38_QUERY_LOG_SAMPLING.md BENCHMARK.md
rg -q 'CHU38_QUERY_LOG_SAMPLING.md' README.md QUERY_HISTORY.md INSPIRATION.md ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q 'CH-U38 \\| Sampled query-log export \\| Implemented' PRODUCT_IDEA_GAPS.md
rg -q 'make benchmark-chu38' BENCHMARK.md CHU38_QUERY_LOG_SAMPLING.md
printf '%s\n' 'CH-U38 documentation verified'
