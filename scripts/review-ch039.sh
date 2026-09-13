#!/usr/bin/env bash
set -euo pipefail

git status --short
git diff --stat
git diff -- Makefile README.md BENCHMARK.md INSPIRATION_BACKLOG.md SQL_AUTO_COUNT_DISTINCT.md hat/hatSql/query.go hat/hatSql/approx_aggregate.go hat/hatSql/auto_distinct.go hat/hatSql/auto_distinct_test.go
