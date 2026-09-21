#!/usr/bin/env bash
set -euo pipefail

git add -- \
	Makefile \
	README.md \
	ENGINE_IDEAS.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	TT021_PACKED_RTREE.md \
	hat/hatDataStructure/tt021_packed_rtree.go \
	hat/hatDataStructure/tt021_packed_rtree_baseline_test.go \
	hat/hatDataStructure/tt021_packed_rtree_benchmark_test.go \
	hat/hatDataStructure/tt021_packed_rtree_contract_test.go \
	hat/hatDataStructure/tt021_packed_rtree_test.go \
	scripts/benchmark-c246.sh \
	scripts/format-c246.sh \
	scripts/race-c246.sh \
	scripts/test-c246.sh \
	scripts/commit-c246-tt021-packed-rtree.sh \
	scripts/push-c246-tt021-packed-rtree.sh \
	scripts/stage-c246-tt021-packed-rtree.sh
git diff --cached --check
git status --short
