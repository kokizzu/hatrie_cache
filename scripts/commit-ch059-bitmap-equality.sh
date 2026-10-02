#!/usr/bin/env bash
set -euo pipefail

test ! -e hat/hatSql/dataflow_ir.go
test ! -e hat/hatSql/round62_build_prereqs.go
git diff --check
git add BENCHMARK.md CH059_BITMAP_EQUALITY_CONTAINER_SCAN.md ENGINE_IDEAS.md INSPIRATION_BACKLOG.md Makefile hat/hatCache/ch059_bitmap_equality_benchmark_test.go hat/hatCache/ch059_bitmap_equality_test.go hat/hatCache/sql_query.go scripts/benchmark-ch059-bitmap-equality.sh scripts/commit-ch059-bitmap-equality.sh scripts/format-ch059-bitmap-equality.sh scripts/push-ch059-bitmap-equality.sh scripts/test-ch059-bitmap-equality.sh scripts/verify-ch059-bitmap-equality.sh
git diff --cached --check
git diff --cached --stat
git commit -m 'feat: optimize bitmap equality traversal [skip ci]'
