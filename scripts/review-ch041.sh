#!/usr/bin/env bash
set -euo pipefail
git status --short
git diff --stat
git diff -- Makefile README.md BENCHMARK.md INSPIRATION_BACKLOG.md hat/hatSql/aggregate_collections.go hat/hatSql/aggregate_collection_test.go
