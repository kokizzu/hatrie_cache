#!/usr/bin/env bash
set -eu

git add \
	Makefile \
	README.md \
	INSPIRATION.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	INCREMENTAL_BOUNDARY_WINDOW.md \
	hat/hatSql/incremental_boundary_window.go \
	hat/hatSql/m065e_incremental_boundary_window_test.go \
	hat/hatSql/m065e_incremental_boundary_window_benchmark_test.go \
	hat/hatSql/m065e_incremental_boundary_window_example_test.go \
	scripts/test-m065e-incremental-boundary-window.sh \
	scripts/format-m065e-incremental-boundary-window.sh \
	scripts/test-race-m065e-incremental-boundary-window.sh \
	scripts/vet-m065e-incremental-boundary-window.sh \
	scripts/benchmark-m065e-incremental-boundary-window.sh \
	scripts/review-m065e-incremental-boundary-window.sh \
	scripts/commit-m065e-incremental-boundary-window.sh \
	scripts/push-m065e-incremental-boundary-window.sh
git commit -m "feat(sql): add incremental boundary windows"
