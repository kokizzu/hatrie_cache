#!/usr/bin/env bash
set -euo pipefail

temporary_worktree=$(mktemp -d /tmp/hatrie-cache-ch037-before.XXXXXX)
repository_root=$(pwd)
cleanup() {
	git worktree remove --force "$temporary_worktree" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git worktree add --detach "$temporary_worktree" HEAD >/dev/null
cp "$repository_root/hat/hatSql/ch037_left_array_join_benchmark_test.go" "$temporary_worktree/hat/hatSql/"
cd "$temporary_worktree"
go test ./hat/hatSql -list 'BenchmarkCH037(ArrayJoinInner|LeftArrayJoinLegacy)'
go test -v ./hat/hatSql -run '^$' -bench '^BenchmarkCH037(ArrayJoinInner|LeftArrayJoinLegacy)$' -benchmem -count=5
