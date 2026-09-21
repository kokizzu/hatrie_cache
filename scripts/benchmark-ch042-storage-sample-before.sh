#!/usr/bin/env bash
set -euo pipefail

temporary_worktree=$(mktemp -d /tmp/hatrie-cache-ch042-before.XXXXXX)
cleanup() {
	git worktree remove --force "$temporary_worktree" >/dev/null 2>&1 || true
}
trap cleanup EXIT
git worktree add --detach "$temporary_worktree" HEAD >/dev/null
cd "$temporary_worktree"
go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLTableSample$' -benchmem -count=5
