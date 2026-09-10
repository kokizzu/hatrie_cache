#!/usr/bin/env bash
set -euo pipefail

git diff --check
git add \
	Makefile \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
	INCREMENTAL_FRAME_WINDOW.md \
	INCREMENTAL_MUTABLE_FRAME_WINDOW.md \
	INSPIRATION.md \
	README.md \
	hat/hatSql/incremental_frame_window.go \
	hat/hatSql/m065l_mutable_frame_window_example_test.go \
	hat/hatSql/m065l_mutable_frame_window_benchmark_test.go \
	hat/hatSql/m065l_mutable_frame_window_test.go \
	hat/hatSql/mutable_frame_window.go \
	scripts/benchmark-m065l-mutable-frame.sh \
	scripts/commit-m065l-mutable-frame.sh \
	scripts/format-m065l-mutable-frame.sh \
	scripts/push-m065l-mutable-frame.sh \
	scripts/review-m065l-mutable-frame.sh \
	scripts/test-m065l-mutable-frame.sh \
	scripts/test-race-m065l-mutable-frame.sh \
	scripts/vet-m065l-mutable-frame.sh
git diff --cached --check
git commit -m "Add mutable incremental frame maintenance"
