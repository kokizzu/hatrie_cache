#!/usr/bin/env bash
set -euo pipefail

git add \
	Makefile \
	README.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	INSPIRATION_ROUND2.md \
	BENCHMARK.md \
	T203_LEADER_WRITE_FENCE.md \
	hat/hatReplication/t203_leader_write_fence.go \
	hat/hatReplication/t203_leader_write_fence_test.go \
	hat/hatReplication/t203_leader_write_fence_benchmark_test.go \
	scripts/benchmark-t203.sh \
	scripts/test-t203.sh \
	scripts/format-t203.sh \
	scripts/race-t203.sh \
	scripts/vet-t203.sh \
	scripts/test-t203-package.sh \
	scripts/verify-docs-t203.sh \
	scripts/stage-t203.sh \
	scripts/commit-t203.sh \
	scripts/push-t203.sh

git diff --cached --check
git status --short
