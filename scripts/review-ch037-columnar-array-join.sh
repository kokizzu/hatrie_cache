#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff --stat -- \
	Makefile \
	README.md \
	BENCHMARK.md \
	ENGINE_IDEAS.md \
	CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
	CH037_COLUMNAR_ARRAY_JOIN.md \
	hat/hatSql/query.go \
	hat/hatSql/columnar_array_join.go \
	hat/hatSql/ch037_columnar_array_join_test.go \
	scripts
git status --short
