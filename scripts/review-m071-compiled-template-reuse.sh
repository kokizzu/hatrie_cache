#!/bin/sh
set -eu

git status --short
git diff --check
git diff --stat
git diff -- Makefile README.md hat/hatSql/compiled.go hat/hatSql/query.go hat/hatSql/compiled_template_reuse_test.go COMPILED_TEMPLATE_REUSE.md ADOPTED_QUERY_ENGINE_IDEAS.md INSPIRATION.md CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md BENCHMARK.md
