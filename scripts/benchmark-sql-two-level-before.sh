#!/bin/sh
set -eu

worktree="$(mktemp -d /tmp/hatrie-two-level-before.XXXXXX)"
cleanup() {
	git worktree remove --force "$worktree"
}
trap cleanup EXIT INT TERM

git worktree add --detach "$worktree" HEAD
cp hat/hatSql/*.go "$worktree/hat/hatSql/"
git -C "$worktree" checkout HEAD -- hat/hatSql/columnar_vector_group_aggregate.go hat/hatSql/hash_group_aggregate.go
(cd "$worktree" && go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLColumnarTwoLevelGroupAggregate$' -benchmem -benchtime "${BENCHTIME:-1s}" -count=1)
