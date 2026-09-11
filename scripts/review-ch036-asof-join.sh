#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff --cached --check
git status --short
git diff --stat
git diff --cached --stat
git grep -n -E 'CH-036|ASOF JOIN|SQL GROUP BY Key Limit' -- README.md ENGINE_IDEAS.md INSPIRATION.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md || true
git diff -- hat/hatSql/query.go hat/hatSql/asof_join.go hat/hatSql/asof_join_test.go README.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md Makefile
