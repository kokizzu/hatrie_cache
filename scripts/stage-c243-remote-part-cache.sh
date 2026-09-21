#!/usr/bin/env bash
set -euo pipefail

git add -- \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	C243_REMOTE_PART_CACHE.md \
	INSPIRATION_ROUND2.md \
	Makefile \
	README.md \
	hat/hatStorage/remote_part_cache_c243_benchmark_test.go \
	hat/hatStorage/remote_part_cache_c243_test.go \
	scripts/benchmark-c243-isolated.sh \
	scripts/commit-c243-remote-part-cache.sh \
	scripts/format-c243-isolated.sh \
	scripts/push-c243-remote-part-cache.sh \
	scripts/stage-c243-remote-part-cache.sh \
	scripts/test-c243-isolated.sh

git diff --cached --check
git diff --cached --name-status
