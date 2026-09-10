#!/usr/bin/env bash
set -eu

git add \
	Makefile \
	README.md \
	INSPIRATION.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	INCREMENTAL_FRAME_WINDOW.md \
	hat/hatSql/incremental_frame_window.go \
	hat/hatSql/m065d_incremental_frame_window_test.go \
	hat/hatSql/m065d_incremental_frame_window_benchmark_test.go \
	hat/hatSql/m065d_incremental_frame_window_example_test.go \
	scripts/test-m065d-incremental-frame-window.sh \
	scripts/format-m065d-incremental-frame-window.sh \
	scripts/test-race-m065d-incremental-frame-window.sh \
	scripts/vet-m065d-incremental-frame-window.sh \
	scripts/benchmark-m065d-incremental-frame-window.sh \
	scripts/review-m065d-incremental-frame-window.sh \
	scripts/commit-m065d-incremental-frame-window.sh \
	scripts/push-m065d-incremental-frame-window.sh
git commit -m "feat(sql): add incremental bounded frame windows"
