#!/usr/bin/env bash
set -euo pipefail

git add \
	BENCHMARK.md \
	Makefile \
	PRODUCT_IDEA_GAPS.md \
	README.md \
	TU18_VOLATILE_CACHE.md \
	hat/hatDataStructure/tu18_volatile_cache_baseline_benchmark_test.go \
	hat/hatDataStructure/tu18_volatile_cache_benchmark_test.go \
	hat/hatDataStructure/tu18_volatile_cache_test.go \
	hat/hatDataStructure/volatile_cache.go \
	scripts/benchmark-tu18-before.sh \
	scripts/benchmark-tu18.sh \
	scripts/commit-tu18.sh \
	scripts/format-tu18.sh \
	scripts/push-tu18.sh \
	scripts/test-tu18.sh \
	scripts/verify-tu18.sh

git commit -m "feat(cache): add opt-in volatile cache engine"
