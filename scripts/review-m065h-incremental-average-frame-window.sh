#!/usr/bin/env bash
set -eu

git diff --check
git status --short
git diff --stat -- \
    Makefile README.md INSPIRATION.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md \
    INCREMENTAL_FRAME_WINDOW.md INCREMENTAL_AVERAGE_FRAME_WINDOW.md \
    hat/hatSql/incremental_frame_window.go \
    hat/hatSql/m065h_incremental_average_frame_window_test.go \
    hat/hatSql/m065h_incremental_average_frame_window_benchmark_test.go \
    hat/hatSql/m065h_incremental_average_frame_window_example_test.go \
    scripts/test-m065h-incremental-average-frame-window.sh \
    scripts/format-m065h-incremental-average-frame-window.sh \
    scripts/benchmark-m065h-incremental-average-frame-window.sh \
    scripts/test-race-m065h-incremental-average-frame-window.sh \
    scripts/vet-m065h-incremental-average-frame-window.sh \
    scripts/review-m065h-incremental-average-frame-window.sh \
    scripts/commit-m065h-incremental-average-frame-window.sh \
    scripts/push-m065h-incremental-average-frame-window.sh
