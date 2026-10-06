#!/usr/bin/env bash
set -euo pipefail

git add -- \
	BENCHMARK.md \
	CONFIG_WATCH_REPLAY_INDEX.md \
	INSPIRATION.md \
	PRODUCT_IDEA_GAPS.md \
	README.md \
	Makefile \
	hat/hatTopology/config_watch.go \
	hat/hatTopology/config_watch_index_benchmark_test.go \
	hat/hatTopology/config_watch_index_test.go \
	scripts/benchmark-config-watch-index.sh \
	scripts/format-config-watch-index.sh \
	scripts/stage-config-watch-index.sh \
	scripts/test-config-watch-index.sh \
	scripts/verify-config-watch-index.sh
git diff --cached --check
git diff --cached --stat
