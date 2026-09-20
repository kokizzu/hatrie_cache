#!/usr/bin/env bash
set -euo pipefail

paths=(
	BENCHMARK.md
	Makefile
	PRODUCT_IDEA_GAPS.md
	README.md
	TU32_FIBER_LOCAL_STORAGE.md
	hat/hatFiber/local.go
	hat/hatFiber/scheduler.go
	hat/hatFiber/tu32_example_test.go
	hat/hatFiber/tu32_local_benchmark_test.go
	hat/hat/hatFiber/tu32_local_test.go
	scripts/benchmark-tu32.sh
	scripts/commit-tu32.sh
	scripts/format-tu32.sh
	scripts/push-tu32.sh
	scripts/race-tu32.sh
	scripts/review-tu32.sh
	scripts/stage-tu32.sh
	scripts/test-tu32.sh
	scripts/vet-tu32.sh
)

git diff --check -- "${paths[@]}"
git diff --cached --check -- "${paths[@]}"
git status --short --untracked-files=all
git diff --stat -- "${paths[@]}"
git diff --cached --stat -- "${paths[@]}"
