#!/usr/bin/env bash
set -euo pipefail

for path in \
	CHU15_VECTORIZED_DECIMAL_KERNELS.md \
	README.md \
	BENCHMARK.md \
	PRODUCT_IDEA_GAPS.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md
do
	test -s "$path"
done

rg -n 'CHU15_VECTORIZED_DECIMAL_KERNELS.md|CH-U15 Fixed-Width Decimal Kernels' README.md BENCHMARK.md PRODUCT_IDEA_GAPS.md ADOPTED_QUERY_ENGINE_IDEAS.md
