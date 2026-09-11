#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff --cached --check
git status --short
git diff --stat
git diff --cached --stat
git diff -- hat/hatSql/query.go hat/hatSql/hash_group_aggregate.go hat/hatSql/columnar_vector_group_aggregate.go hat/hatSql/sql_result_cache.go hat/hatSql/governance.go hat/hatSql/columnar_vector_group_aggregate_test.go README.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md Makefile
