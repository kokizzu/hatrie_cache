#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

git add \
	BENCHMARK.md \
	Makefile \
	PRODUCT_IDEA_GAPS.md \
	README.md \
	TU17_VINYL_LSM_TABLE.md \
	hat/hatDataStructure/lsm_table.go \
	hat/hatDataStructure/tu17_lsm_baseline_benchmark_test.go \
	hat/hatDataStructure/tu17_lsm_table_benchmark_test.go \
	hat/hatDataStructure/tu17_lsm_table_test.go \
	scripts/benchmark-tu17-before.sh \
	scripts/benchmark-tu17.sh \
	scripts/commit-tu17.sh \
	scripts/format-tu17.sh \
	scripts/push-tu17.sh \
	scripts/test-tu17.sh \
	scripts/verify-tu17.sh
git commit -m "feat(storage): add opt-in vinyl-style lsm table"
