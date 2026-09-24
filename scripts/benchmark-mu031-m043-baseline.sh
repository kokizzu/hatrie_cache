#!/usr/bin/env bash
set -euo pipefail

worktree=/tmp/hatrie-cache-m043-baseline
if [[ -e "$worktree" ]]; then
  git worktree remove --force "$worktree"
fi
cleanup() {
  git worktree remove --force "$worktree" >/dev/null 2>&1 || true
}
trap cleanup EXIT
git worktree add --detach "$worktree" origin/master >/dev/null
cp hat/hatSql/mu031_retractable_aggregate_baseline_benchmark_test.go "$worktree/hat/hatSql/"
cd "$worktree"
go test ./hat/hatSql -run '^$' -bench 'BenchmarkMU031AggregateDirectAdd$' -benchmem -count=5
