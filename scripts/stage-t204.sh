#!/usr/bin/env bash
set -euo pipefail

git add \
	Makefile \
	README.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	INSPIRATION_ROUND2.md \
	BENCHMARK.md \
	T204_SUPERVISED_FAILOVER.md \
	hat/hatReplication/t204_supervised_failover.go \
	hat/hatReplication/t204_supervised_failover_test.go \
	hat/hatReplication/t204_supervised_failover_benchmark_test.go \
	scripts/benchmark-t204.sh \
	scripts/test-t204.sh \
	scripts/format-t204.sh \
	scripts/race-t204.sh \
	scripts/vet-t204.sh \
	scripts/test-t204-package.sh \
	scripts/verify-docs-t204.sh \
	scripts/stage-t204.sh \
	scripts/commit-t204.sh \
	scripts/push-t204.sh

git diff --cached --check
git status --short
