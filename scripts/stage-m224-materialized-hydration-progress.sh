#!/usr/bin/env bash
set -euo pipefail

git add -- \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	INSPIRATION_ROUND2.md \
	M224_MATERIALIZED_HYDRATION_PROGRESS.md \
	Makefile \
	hat/hatSql/contracts.go \
	hat/hatSql/materialized.go \
	hat/hatSql/materialized_hydration.go \
	hat/hatSql/materialized_hydration_progress.go \
	hat/hatSql/m224_materialized_hydration_progress_benchmark_test.go \
	hat/hatSql/m224_materialized_hydration_progress_test.go \
	hat/hatSql/query.go \
	scripts/m224-materialized-hydration-progress.sh \
	scripts/stage-m224-materialized-hydration-progress.sh \
	scripts/commit-m224-materialized-hydration-progress.sh \
	scripts/push-m224-materialized-hydration-progress.sh
