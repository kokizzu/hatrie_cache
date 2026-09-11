#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff -- ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md ENGINE_IDEAS.md Makefile README.md hat/hatSql/collation.go hat/hatSql/index_advisor.go hat/hatSql/m052p_auto_native_dataflow.go hat/hatSql/prewhere.go hat/hatSql/query.go hat/hatSql/rewrite.go hat/hatSql/subquery.go
git diff --stat
git status --short
