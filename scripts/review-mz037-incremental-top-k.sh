#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff --stat -- \
	Makefile \
	hat/hatSql/c213_incremental_top_k.go \
	hat/hatSql/c213_incremental_top_k_test.go \
	hat/hatSql/c213_incremental_top_k_benchmark_test.go \
	C213_INCREMENTAL_TOP_K.md \
	ENGINE_IDEAS.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	README.md \
	scripts
