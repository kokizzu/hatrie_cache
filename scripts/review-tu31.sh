#!/usr/bin/env bash
set -euo pipefail

paths=(
	BENCHMARK.md
	Makefile
	PRODUCT_IDEA_GAPS.md
	README.md
	TU31_FIBER_CHANNELS_AND_CONDITIONS.md
	hat/hatFiber/channel.go
	hat/hatFiber/scheduler.go
	hat/hatFiber/sync.go
	hat/hatFiber/tu31_example_test.go
	hat/hatFiber/tu31_sync_benchmark_test.go
	hat/hat/hatFiber/tu31_sync_test.go
	scripts/benchmark-tu31.sh
	scripts/commit-tu31.sh
	scripts/format-tu31.sh
	scripts/push-tu31.sh
	scripts/race-tu31.sh
	scripts/review-tu31.sh
	scripts/stage-tu31.sh
	scripts/test-tu31.sh
	scripts/vet-tu31.sh
)

git diff --check -- "${paths[@]}"
git diff --cached --check -- "${paths[@]}"
git status --short --untracked-files=all
git diff --stat -- "${paths[@]}"
git diff --cached --stat -- "${paths[@]}"
