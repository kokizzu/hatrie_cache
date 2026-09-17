#!/usr/bin/env bash
set -euo pipefail

git diff --check
git status --short
git diff --stat -- \
	Makefile \
	README.md \
	BENCHMARK.md \
	PRODUCT_IDEA_GAPS.md \
	CHU48_RANKED_FULL_TEXT.md \
	hat/hatDataStructure/ch025_token_postings_index.go \
	hat/hatDataStructure/chu48_ranked_token_postings_index.go \
	hat/hatDataStructure/chu48_ranked_token_postings_index_benchmark_test.go \
	hat/hatDataStructure/chu48_ranked_token_postings_index_test.go \
	scripts/benchmark-chu48-ranked-full-text.sh \
	scripts/cleanup-hatrie-tmp.sh \
	scripts/format-chu48-ranked-full-text.sh \
	scripts/race-chu48-ranked-full-text.sh \
	scripts/review-chu48-ranked-full-text.sh \
	scripts/test-chu48-ranked-full-text.sh \
	scripts/test-chu48-ranked-package.sh \
	scripts/vet-chu48-ranked-full-text.sh
