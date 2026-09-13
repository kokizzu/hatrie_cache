#!/usr/bin/env bash
set -euo pipefail

git add -- \
	BENCHMARK.md \
	INSPIRATION_BACKLOG.md \
	Makefile \
	README.md \
	REMOTE_PART_PREFETCH.md \
	hat/hatStorage/remote_part_cache.go \
	hat/hatStorage/remote_part_cache_baseline_test.go \
	hat/hatStorage/remote_part_cache_test.go \
	scripts/benchmark-remote-part-prefetch-after.sh \
	scripts/commit-remote-part-prefetch.sh \
	scripts/push-remote-part-prefetch.sh \
	scripts/review-remote-part-prefetch.sh
git diff --cached --check
git commit -m "feat: add bounded remote-part prefetch"
