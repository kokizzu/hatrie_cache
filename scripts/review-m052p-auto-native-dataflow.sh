#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff --stat
git diff -- hat/hatSql/query.go ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md INSPIRATION.md Makefile scripts/inspect-query-engine-backlog.sh
git status --short
