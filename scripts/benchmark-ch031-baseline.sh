#!/usr/bin/env bash
set -euo pipefail

worktree=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-ch031-baseline.XXXXXX")
cleanup() {
  git worktree remove --force "$worktree" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git worktree add --detach "$worktree" HEAD >/dev/null
cp hat/hatSql/query_manager_execute_benchmark_test.go "$worktree/hat/hatSql/query_manager_execute_benchmark_test.go"
(cd "$worktree" && go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH031SQLQueryManagerDefaultExecute$' -benchmem -benchtime=1s -count=5)
