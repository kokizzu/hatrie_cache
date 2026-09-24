#!/usr/bin/env bash
set -euo pipefail

git add -- \
	Makefile \
	README.md \
	ENGINE_IDEAS.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	TT021_MATERIALIZED_SPATIAL_INDEX.md \
	hat/hatSchema/materialized.go \
	hat/hatSchema/spatial_index.go \
	hat/hatSchema/tt021_materialized_spatial_index_test.go \
	hat/hatSchema/tt021_materialized_spatial_index_benchmark_test.go \
	scripts/format-tt021-materialized-spatial-index.sh \
	scripts/test-tt021-materialized-spatial-index.sh \
	scripts/benchmark-tt021-materialized-spatial-index.sh \
	scripts/race-tt021-materialized-spatial-index.sh \
	scripts/vet-tt021-materialized-spatial-index.sh \
	scripts/status-tt021-materialized-spatial-index.sh \
	scripts/stage-tt021-materialized-spatial-index.sh \
	scripts/commit-tt021-materialized-spatial-index.sh \
	scripts/push-tt021-materialized-spatial-index.sh
git diff --cached --check
git diff --cached --stat
