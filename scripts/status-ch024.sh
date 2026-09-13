#!/usr/bin/env bash
set -eu

git diff --check
git status --short
rg -n 'CH-24|CH024|residual_false_positive|false positive' INSPIRATION_BACKLOG.md README.md CH024_SKIP_INDEX_USEFULNESS.md BENCHMARK.md hat/hatSql/query.go hat/hatSql/columnar_segment_skip_test.go
