#!/usr/bin/env bash
set -euo pipefail

root=$(git rev-parse --show-toplevel)
worktree=$(mktemp -d /tmp/hatrie-c231-baseline.XXXXXX)
cleanup() {
	git -C "$root" worktree remove --force "$worktree" >/dev/null 2>&1 || true
	rm -rf "$worktree"
}
trap cleanup EXIT

git -C "$root" fetch origin master
git -C "$root" worktree add --detach "$worktree" origin/master >/dev/null
mkdir -p "$worktree/hat/hatWorkload"
cp "$root/hat/hatWorkload/c231_memory_admission_baseline_benchmark_test.go" "$worktree/hat/hatWorkload/"
git -C "$worktree" status --short
go -C "$worktree" test ./hat/hatWorkload -run '^$' -bench '^BenchmarkC231ExistingAdmissionAcquireRelease$' -benchmem -count=5
