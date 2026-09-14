#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff --stat -- \
	Makefile \
	hat/hatSql/m039_incremental_distinct.go \
	hat/hatSql/m039_incremental_distinct_test.go \
	hat/hatSql/m039_incremental_distinct_benchmark_test.go \
	MZ039_INCREMENTAL_DISTINCT.md \
	ENGINE_IDEAS.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	README.md \
	scripts
