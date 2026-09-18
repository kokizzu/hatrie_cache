#!/usr/bin/env bash
set -euo pipefail

git add \
	Makefile \
	README.md \
	BENCHMARK.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	INSPIRATION_BACKLOG.md \
	MZ028_TEMPORAL_INTERVAL_ARRANGEMENT.md \
	hat/hatSql/mz028_temporal_interval_arrangement.go \
	hat/hatSql/mz028_temporal_interval_arrangement_test.go \
	hat/hatSql/mz028_temporal_interval_arrangement_baseline_benchmark_test.go \
	hat/hatSql/mz028_temporal_interval_arrangement_benchmark_test.go \
	scripts/test-mz028-temporal-arrangement.sh \
	scripts/format-mz028-temporal-arrangement.sh \
	scripts/benchmark-mz028-temporal-arrangement.sh \
	scripts/test-mz028-temporal-arrangement-package.sh \
	scripts/race-mz028-temporal-arrangement.sh \
	scripts/vet-mz028-temporal-arrangement.sh \
	scripts/verify-mz028-temporal-arrangement-docs.sh \
	scripts/stage-mz028-temporal-arrangement.sh \
	scripts/commit-mz028-temporal-arrangement.sh \
	scripts/push-mz028-temporal-arrangement.sh

git diff --cached --check
git diff --cached --stat
git status --short
