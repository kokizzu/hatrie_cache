#!/usr/bin/env bash
set -eu

git add ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md INCREMENTAL_OFFSET_WINDOW.md INSPIRATION.md Makefile README.md hat/hatSql/incremental_offset_window.go hat/hatSql/m065c_incremental_offset_window_benchmark_test.go hat/hatSql/m065c_incremental_offset_window_test.go scripts/audit-adoption-next-idea.sh scripts/benchmark-m065c-incremental-offset-window.sh scripts/commit-m065c-incremental-offset-window.sh scripts/format-m065c-incremental-offset-window.sh scripts/push-m065c-incremental-offset-window.sh scripts/review-m065c-incremental-offset-window.sh scripts/test-m065c-incremental-offset-window.sh scripts/test-race-m065c-incremental-offset-window.sh scripts/vet-m065c-incremental-offset-window.sh
git diff --cached --check
git commit -m 'feat(sql): add incremental lag and lead windows'
