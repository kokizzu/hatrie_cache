#!/usr/bin/env bash
set -euo pipefail

git add \
	Makefile \
	README.md \
	BENCHMARK.md \
	INSPIRATION_ROUND2.md \
	T237_CONNECTION_POOL_HEALTH.md \
	hat/hatPeer/connection_pool.go \
	hat/hatPeer/t237_health_baseline_benchmark_test.go \
	hat/hatPeer/t237_connection_pool_health_test.go \
	scripts/benchmark-t237-before.sh \
	scripts/benchmark-t237.sh \
	scripts/format-t237.sh \
	scripts/race-t237.sh \
	scripts/test-t237-package-verbose.sh \
	scripts/test-t237-package.sh \
	scripts/test-t237.sh \
	scripts/vet-t237.sh \
	scripts/stage-t237.sh \
	scripts/commit-t237.sh \
	scripts/push-t237.sh

git diff --cached --check
