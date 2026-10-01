#!/usr/bin/env bash
set -euo pipefail

git add BENCHMARK.md PRODUCT_IDEA_GAPS.md README.md TU10_JOURNAL_WRITE_QUORUM.md Makefile \
	hat/hatReplication/journal_write_quorum.go \
	hat/hatReplication/journal_write_quorum_test.go \
	hat/hatReplication/journal_write_quorum_benchmark_test.go \
	scripts/benchmark-t-u10-state.sh scripts/format-t-u10-state.sh \
	scripts/commit-t-u10-state.sh scripts/push-t-u10-state.sh \
	scripts/race-t-u10-state.sh scripts/review-t-u10-state.sh \
	scripts/stage-t-u10-state.sh scripts/test-t-u10-package.sh \
	scripts/test-t-u10-state.sh scripts/vet-t-u10-state.sh
git diff --cached --check
