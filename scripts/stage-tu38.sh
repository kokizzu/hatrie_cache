#!/usr/bin/env bash
set -euo pipefail

git add \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	Makefile \
	PRODUCT_IDEA_GAPS.md \
	TR038_CONFLICT_INTROSPECTION.md \
	hat/hatReplication/tu38_conflict_introspection.go \
	hat/hatReplication/tu38_conflict_introspection_test.go \
	scripts/benchmark-tu38-baseline.sh \
	scripts/benchmark-tu38.sh \
	scripts/commit-tu38.sh \
	scripts/format-tu38.sh \
	scripts/push-tu38.sh \
	scripts/race-tu38.sh \
	scripts/review-tu38.sh \
	scripts/stage-tu38.sh \
	scripts/test-tu38-full.sh \
	scripts/test-tu38-red.sh \
	scripts/test-tu38.sh \
	scripts/vet-tu38.sh
git diff --cached --check
git diff --cached --stat
