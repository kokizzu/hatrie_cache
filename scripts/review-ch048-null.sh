#!/usr/bin/env bash
set -euo pipefail

git status --short
git diff --check -- BENCHMARK.md CH048_NULL_PREDICATE.md CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md ENGINE_IDEAS.md hat/hatSql/columnar_null_predicate.go hat/hatSql/ch048_null_predicate_test.go scripts
git diff --stat -- BENCHMARK.md CH048_NULL_PREDICATE.md CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md ENGINE_IDEAS.md hat/hatSql/columnar_null_predicate.go hat/hatSql/ch048_null_predicate_test.go scripts
