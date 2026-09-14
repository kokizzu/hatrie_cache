#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff --stat -- \
	Makefile \
	hat/hatSql/m040_incremental_percentile.go \
	hat/hatSql/m040_incremental_percentile_test.go \
	hat/hatSql/m040_incremental_percentile_benchmark_test.go \
	MZ040_INCREMENTAL_PERCENTILE.md \
	ENGINE_IDEAS.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	README.md \
	scripts
