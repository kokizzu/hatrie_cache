#!/usr/bin/env bash
set -euo pipefail

git add -- \
	Makefile \
	README.md \
	ENGINE_IDEAS.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	TT018_PAGE_INDEX_RESIDENCY.md \
	hat/hatDataStructure/tt018_page_index_residency.go \
	hat/hatDataStructure/tt018_page_index_residency_test.go \
	hat/hatDataStructure/tt018_page_index_residency_baseline_test.go \
	hat/hatDataStructure/tt018_page_index_residency_benchmark_test.go \
	scripts/benchmark-tt018.sh \
	scripts/format-tt018.sh \
	scripts/race-tt018.sh \
	scripts/test-tt018.sh \
	scripts/commit-tt018-page-index-residency.sh \
	scripts/push-tt018-page-index-residency.sh \
	scripts/stage-tt018-page-index-residency.sh
git diff --cached --check
git status --short
