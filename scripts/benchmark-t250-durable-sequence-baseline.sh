#!/usr/bin/env bash
set -euo pipefail

root=$(git rev-parse --show-toplevel)
worktree=/tmp/hatrie-t250-durable-sequence-baseline.$$
trap 'git -C "$root" worktree remove --force "$worktree" >/dev/null 2>&1 || true; rm -rf "$worktree"' EXIT

git -C "$root" fetch origin
git -C "$root" worktree add --detach "$worktree" origin/master
cp "$root/hat/hatDataStructure/t250_durable_sequence_baseline_benchmark_test.go" "$worktree/hat/hatDataStructure/t250_durable_sequence_baseline_benchmark_test.go"
go test "$worktree/hat/hatDataStructure/t250_durable_sequence_baseline_benchmark_test.go" -bench '^BenchmarkT250BaselineAtomicAdd$' -benchmem -count=5
