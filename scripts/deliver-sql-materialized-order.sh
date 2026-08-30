#!/usr/bin/env sh
set -eu

git add -- \
	BENCHMARK.md \
	INDEX_PROPOSAL.md \
	Makefile \
	hat/hatCache/sql_production_test.go \
	hat/hatCache/sql_typed_index_baseline_benchmark_test.go \
	hat/hatSql/query.go \
	scripts/deliver-sql-materialized-order.sh \
	scripts/format-sql-materialized-order.sh \
	scripts/inspect-sql-execution-budget.sh \
	scripts/inspect-sql-materialized-order.sh \
	scripts/test-sql-materialized-order.sh
git diff --cached --check
git commit -m "perf(sql): stream indexed order limit results"
git push
