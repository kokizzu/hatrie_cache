#!/usr/bin/env bash
set -euo pipefail

git add \
	Makefile \
	README.md \
	BENCHMARK.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	INSPIRATION_ROUND2.md \
	M248_MAINTAINED_RESULT_CACHE.md \
	hat/hatSql/result_cache.go \
	hat/hatSql/m248_maintained_result_cache_test.go \
	hat/hatSql/m248_maintained_result_cache_benchmark_test.go \
	scripts/benchmark-m248.sh \
	scripts/format-m248.sh \
	scripts/race-m248.sh \
	scripts/test-m248.sh \
	scripts/test-m248-package.sh \
	scripts/verify-docs-m248.sh \
	scripts/vet-m248.sh \
	scripts/stage-m248.sh \
	scripts/commit-m248.sh \
	scripts/push-m248.sh

git status --short
