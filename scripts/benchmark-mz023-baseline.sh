#!/bin/sh
set -eu

baseline=/tmp/hatrie-cache-mz023-baseline
if test -e "$baseline"; then
	printf '%s\n' "baseline path already exists: $baseline" >&2
	exit 1
fi
cleanup() {
	git worktree remove "$baseline"
}
trap cleanup EXIT INT TERM
git worktree add --detach "$baseline" HEAD
GOMAXPROCS=1 go test -C "$baseline" ./hat/hatSql -run '^$' -bench '^BenchmarkSQLSinkCommitCoordinator(NewCommit|DuplicateCommit)$' -benchmem -benchtime=100ms -count=5 -timeout 120s
