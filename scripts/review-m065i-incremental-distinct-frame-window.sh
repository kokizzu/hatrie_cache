#!/usr/bin/env bash
set -eu

git diff --check
git status --short
git diff --stat -- \
    Makefile README.md INSPIRATION.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md \
    INCREMENTAL_FRAME_WINDOW.md INCREMENTAL_DISTINCT_FRAME_WINDOW.md \
    hat/hatSql/incremental_frame_window.go \
    hat/hatSql/m065i_incremental_distinct_frame_window_test.go \
    hat/hatSql/m065i_incremental_distinct_frame_window_benchmark_test.go \
    hat/hatSql/m065i_incremental_distinct_frame_window_example_test.go \
    scripts/test-m065i-incremental-distinct-frame-window.sh \
    scripts/format-m065i-incremental-distinct-frame-window.sh \
    scripts/benchmark-m065i-incremental-distinct-frame-window.sh \
    scripts/test-race-m065i-incremental-distinct-frame-window.sh \
    scripts/vet-m065i-incremental-distinct-frame-window.sh \
    scripts/review-m065i-incremental-distinct-frame-window.sh \
    scripts/commit-m065i-incremental-distinct-frame-window.sh \
    scripts/push-m065i-incremental-distinct-frame-window.sh
