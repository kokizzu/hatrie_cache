#!/usr/bin/env bash
set -euo pipefail

root=$(git rev-parse --show-toplevel)
worktree=/tmp/hatrie_cache_m065t_boolean_predicate_baseline

git worktree remove --force "$worktree" >/dev/null 2>&1 || true
git worktree add --detach "$worktree" HEAD >/dev/null
trap 'git worktree remove --force "$worktree" >/dev/null 2>&1 || true' EXIT

cp "$root/hat/hatSql/columnar_bool_predicate_benchmark_test.go" "$worktree/hat/hatSql/columnar_bool_predicate_benchmark_test.go"
cd "$worktree"
GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLColumnarPackedBooleanPredicate$' -benchmem -benchtime=250ms -count=7
