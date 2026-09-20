#!/usr/bin/env bash
set -euo pipefail

git add \
	BENCHMARK.md \
	Makefile \
	PRODUCT_IDEA_GAPS.md \
	README.md \
	TU24_CONDITIONAL_INDEX_CATALOG.md \
	hat/hatDataStructure/conditional_index_catalog.go \
	hat/hatDataStructure/tu24_conditional_index_catalog_benchmark_test.go \
	hat/hatDataStructure/tu24_conditional_index_catalog_test.go \
	scripts/benchmark-tu24.sh \
	scripts/commit-tu24.sh \
	scripts/format-tu24.sh \
	scripts/push-tu24.sh \
	scripts/test-tu24.sh \
	scripts/verify-tu24.sh

git commit -m "feat(index): add conditional index catalog"
