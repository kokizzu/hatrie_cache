#!/usr/bin/env bash
set -euo pipefail

mkdir -p build/benchmarks
repo_root="$(git rev-parse --show-toplevel)"
worktree="/tmp/hatrie-chu01-baseline-$$"
cleanup() {
	git -C "$repo_root" worktree remove --force "$worktree" >/dev/null 2>&1 || true
}
trap cleanup EXIT
git -C "$repo_root" worktree add --detach "$worktree" HEAD >/dev/null
(cd "$worktree" && go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkAsyncBatcher(Submit|SubmitMaxOne)$' -benchmem -count=5) | tee build/benchmarks/chu01-async-dedup-baseline.txt
