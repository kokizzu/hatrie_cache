#!/usr/bin/env bash
set -euo pipefail

worktree=/tmp/hatrie-cache-m042-baseline
if [[ -e "$worktree" ]]; then
  git worktree remove --force "$worktree"
fi
cleanup() {
  git worktree remove --force "$worktree" >/dev/null 2>&1 || true
}
trap cleanup EXIT
git worktree add --detach "$worktree" origin/master >/dev/null
cp hat/hatSql/tu05_session_transaction_settings_benchmark_test.go "$worktree/hat/hatSql/"
cd "$worktree"
go test ./hat/hatSql -run '^$' -bench 'BenchmarkTU05SessionExecuteBaseline$' -benchmem -count=5
