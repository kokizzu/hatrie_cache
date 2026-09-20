#!/usr/bin/env bash
set -euo pipefail

git add -- \
	BENCHMARK.md \
	Makefile \
	PRODUCT_IDEA_GAPS.md \
	README.md \
	TU34_SPACE_WAL_SYNC_POLICY.md \
	api.go \
	hat/hatCache/journal.go \
	hat/hatCache/tu34_space_sync_policy_benchmark_test.go \
	hat/hatCache/tu34_space_sync_policy_common_benchmark_test.go \
	hat/hatCache/tu34_space_sync_policy_test.go \
	hat/hatJournal/journal.go \
	scripts/benchmark-tu34-baseline.sh \
	scripts/benchmark-tu34.sh \
	scripts/commit-tu34.sh \
	scripts/format-tu34.sh \
	scripts/push-tu34.sh \
	scripts/race-tu34.sh \
	scripts/review-tu34.sh \
	scripts/stage-tu34.sh \
	scripts/test-tu34-journal.sh \
	scripts/test-tu34.sh \
	scripts/vet-tu34.sh
