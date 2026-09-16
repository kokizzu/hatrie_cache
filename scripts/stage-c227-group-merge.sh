#!/bin/sh
set -eu
git add Makefile README.md INSPIRATION_ROUND2.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md C227_GROUP_MERGE_BUDGET.md \
	hat/hatSql/query.go hat/hatSql/sql_result_cache.go \
	hat/hatSql/c227_group_merge_budget_test.go hat/hatSql/c227_group_merge_budget_benchmark_test.go \
	scripts/benchmark-c227-group-merge.sh scripts/format-c227-group-merge.sh scripts/test-c227-group-merge.sh \
	scripts/test-c227-group-merge-package.sh scripts/test-c227-all.sh scripts/race-c227-group-merge.sh scripts/vet-c227-group-merge.sh \
	scripts/verify-c227-group-merge.sh scripts/status-c227-group-merge.sh scripts/stage-c227-group-merge.sh scripts/commit-c227-group-merge.sh scripts/push-c227-group-merge.sh
