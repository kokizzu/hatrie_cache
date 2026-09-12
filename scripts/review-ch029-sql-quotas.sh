#!/bin/sh
set -eu

git diff --check
git status --short
git diff --stat
git diff -- Makefile README.md ENGINE_IDEAS.md BENCHMARK.md
git diff -- hat/hatSql/quota.go hat/hatSql/query.go hat/hatCache/sql_quota.go hat/hatSql/ch029_sql_quota_test.go hat/hatSql/ch029_sql_quota_benchmark_test.go
