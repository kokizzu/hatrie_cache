#!/usr/bin/env bash
set -eu

git add -- \
    Makefile README.md INSPIRATION.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md \
    INCREMENTAL_FRAME_WINDOW.md INCREMENTAL_EXTREMA_FRAME_WINDOW.md \
    hat/hatSql/incremental_frame_window.go \
    hat/hatSql/m065g_incremental_extrema_frame_window_test.go \
    hat/hatSql/m065g_incremental_extrema_frame_window_benchmark_test.go \
    hat/hatSql/m065g_incremental_extrema_frame_window_example_test.go \
    scripts/test-m065g-incremental-extrema-frame-window.sh \
    scripts/format-m065g-incremental-extrema-frame-window.sh \
    scripts/test-race-m065g-incremental-extrema-frame-window.sh \
    scripts/vet-m065g-incremental-extrema-frame-window.sh \
    scripts/benchmark-m065g-incremental-extrema-frame-window.sh \
    scripts/review-m065g-incremental-extrema-frame-window.sh \
    scripts/commit-m065g-incremental-extrema-frame-window.sh \
    scripts/push-m065g-incremental-extrema-frame-window.sh
git diff --cached --check
git commit -m "feat(sql): add incremental min max frame windows"
