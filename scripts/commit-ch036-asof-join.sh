#!/usr/bin/env bash
set -euo pipefail

git add \
	Makefile \
	README.md \
	ENGINE_IDEAS.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	SQL_ASOF_JOIN.md \
	hat/hatSql/query.go \
	hat/hatSql/asof_join.go \
	hat/hatSql/asof_join_test.go \
	hat/hatSql/asof_join_benchmark_test.go \
	hat/hatSql/asof_join_optimized_benchmark_test.go \
	scripts/test-ch036-asof-join.sh \
	scripts/format-ch036-asof-join.sh \
	scripts/benchmark-ch036-asof-join.sh \
	scripts/review-ch036-asof-join.sh \
	scripts/commit-ch036-asof-join.sh \
	scripts/push-ch036-asof-join.sh
git diff --cached --check
git commit -m "feat(sql): add ASOF JOIN"
