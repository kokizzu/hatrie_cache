#!/bin/sh
set -eu

git diff --check
git add \
	Makefile \
	ENGINE_IDEAS.md \
	INSPIRATION.md \
	README.md \
	BENCHMARK.md \
	TT020_ORDERED_INDEX_RANGES.md \
	hat/hatDataStructure/ordered_index.go \
	hat/hatDataStructure/tt020_range_test.go \
	hat/hatDataStructure/tt020_range_benchmark_test.go \
	scripts/test-tt020.sh \
	scripts/stage-tt020.sh \
	scripts/commit-tt020.sh \
	scripts/push-tt020.sh
git diff --cached --check
git diff --cached --name-only
