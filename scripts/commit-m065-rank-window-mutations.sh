#!/usr/bin/env bash
set -eu

git add BENCHMARK.md INCREMENTAL_RANK_WINDOW.md INSPIRATION.md Makefile README.md hat/hatSql/incremental_rank_window.go hat/hatSql/m065_mutable_rank_window_test.go hat/hatSql/m065_rank_window_benchmark_test.go hat/hatSql/mutable_rank_window.go scripts/benchmark-m065-rank-window-mutations.sh scripts/commit-m065-rank-window-mutations.sh scripts/format-m065-rank-window-mutations.sh scripts/push-m065-rank-window-mutations.sh scripts/review-m065-rank-window-mutations.sh scripts/test-m065-rank-window-all.sh scripts/test-m065-rank-window-mutations.sh scripts/test-race-m065-rank-window-mutations.sh scripts/vet-m065-rank-window-mutations.sh
git diff --cached --check
git commit -m 'feat(sql): add mutable rank window maintenance'
