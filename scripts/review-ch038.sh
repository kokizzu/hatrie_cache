#!/usr/bin/env bash
set -euo pipefail
git status --short
git diff --stat
git diff -- Makefile README.md BENCHMARK.md INSPIRATION_BACKLOG.md hat/hatSql/query.go hat/hatSql/bitmap_aggregates.go hat/hatSql/bitmap_aggregate_test.go
