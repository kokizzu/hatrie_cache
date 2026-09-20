#!/usr/bin/env bash
set -euo pipefail

git add \
	BENCHMARK.md \
	Makefile \
	PRODUCT_IDEA_GAPS.md \
	README.md \
	TU25_RTREE_SPACE_CATALOG.md \
	hat/hatDataStructure/rtree_space_catalog.go \
	hat/hatDataStructure/tu25_rtree_space_catalog_benchmark_test.go \
	hat/hatDataStructure/tu25_rtree_space_catalog_test.go \
	scripts/benchmark-tu25-rebuild.sh \
	scripts/benchmark-tu25.sh \
	scripts/commit-tu25.sh \
	scripts/format-tu25.sh \
	scripts/push-tu25.sh \
	scripts/race-tu25.sh \
	scripts/stage-tu25.sh \
	scripts/test-tu25-package.sh \
	scripts/test-tu25.sh \
	scripts/verify-tu25.sh \
	scripts/vet-tu25.sh
git diff --cached --check
git diff --cached --stat
