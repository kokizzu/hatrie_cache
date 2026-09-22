#!/usr/bin/env bash
set -euo pipefail

git add -- \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	INSPIRATION_ROUND2.md \
	M223_MATERIALIZED_VIEW_HYDRATION.md \
	Makefile \
	hat/hatSql/materialized.go \
	hat/hatSql/materialized_hydration.go \
	hat/hatSql/materialized_point_lookup.go \
	hat/hatSql/materialized_point_lookup_build.go \
	hat/hatSql/materialized_point_lookup_retirement.go \
	hat/hatSql/materialized_hydration.go \
	hat/hatSql/m223_materialized_hydration_benchmark_test.go \
	hat/hatSql/m223_materialized_hydration_test.go \
	scripts/benchmark-m223-materialized-hydration.sh \
	scripts/commit-m223-materialized-hydration.sh \
	scripts/format-m223-materialized-hydration.sh \
	scripts/push-m223-materialized-hydration.sh \
	scripts/race-m223-materialized-hydration.sh \
	scripts/stage-m223-materialized-hydration.sh \
	scripts/status-m223-materialized-hydration.sh \
	scripts/test-m223-materialized-hydration.sh \
	scripts/test-m223-related-materialized.sh \
	scripts/vet-m223-materialized-hydration.sh
