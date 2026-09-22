#!/usr/bin/env bash
set -euo pipefail

root=$(pwd)
baseline=$(mktemp -d /tmp/hatrie-m244-baseline.XXXXXX)
cleanup() {
  git worktree remove --force "$baseline"
}
trap cleanup EXIT

git worktree add --detach "$baseline" HEAD
cp "$root/hat/hatSql/m244_compaction_debt_benchmark_test.go" "$baseline/hat/hatSql/m244_compaction_debt_benchmark_test.go"
printf '%s\n' '== M243 parent baseline =='
(cd "$baseline" && go test ./hat/hatSql -run '^$' -bench '^BenchmarkM244CompactionDebtStats$' -benchmem -count=5)
printf '%s\n' '== M244 current =='
go test ./hat/hatSql -run '^$' -bench '^BenchmarkM244CompactionDebtStats$' -benchmem -count=5
