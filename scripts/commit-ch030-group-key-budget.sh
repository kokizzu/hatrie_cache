#!/usr/bin/env bash
set -euo pipefail

git add \
	Makefile \
	README.md \
	ENGINE_IDEAS.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	SQL_GROUP_KEY_LIMIT.md \
	hat/hatSql/query.go \
	hat/hatSql/hash_group_aggregate.go \
	hat/hatSql/columnar_vector_group_aggregate.go \
	hat/hatSql/sql_result_cache.go \
	hat/hatSql/governance.go \
	hat/hatSql/max_group_keys_test.go \
	hat/hatSql/max_group_keys_spill_test.go \
	hat/hatSql/max_group_keys_benchmark_test.go \
	hat/hatSql/max_group_keys_guarded_benchmark_test.go \
	hat/hatSql/columnar_vector_group_aggregate_test.go \
	scripts/test-ch030-group-key-budget.sh \
	scripts/format-ch030-group-key-budget.sh \
	scripts/benchmark-ch030-group-key-budget.sh \
	scripts/review-ch030-group-key-budget.sh \
	scripts/commit-ch030-group-key-budget.sh \
	scripts/push-ch030-group-key-budget.sh
git diff --cached --check
git commit -m "feat(sql): cap GROUP BY key cardinality"
