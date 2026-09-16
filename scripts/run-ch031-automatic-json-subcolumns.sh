#!/usr/bin/env bash
set -euo pipefail

mode=${1:?expected test, verify, or benchmark}
worktree=$(mktemp -d /tmp/hatrie-cache-ch031-auto.XXXXXX)
cleanup() {
	git worktree remove --force "$worktree" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git worktree add --detach "$worktree" HEAD >/dev/null
if [[ ! -e "$worktree/hat/hatSql/asof_join.go" ]]; then
	asof_revision=$(git log -n 1 --format=%H -- hat/hatSql/asof_join.go)
	git show "$asof_revision:hat/hatSql/asof_join.go" > "$worktree/hat/hatSql/asof_join.go"
fi
cp hat/hatSql/ch031_automatic_json_subcolumn.go "$worktree/hat/hatSql/"
cp hat/hatSql/ch031_automatic_json_subcolumn_test.go "$worktree/hat/hatSql/"
cp hat/hatSql/ch031_automatic_json_subcolumn_benchmark_test.go "$worktree/hat/hatSql/"
cd "$worktree"

case "$mode" in
test)
	go test ./hat/hatSql -run '^TestCH031Automatic' -count=1
	;;
verify)
	go test -race ./hat/hatSql -run '^TestCH031Automatic' -count=1
	go vet ./hat/hatSql
	;;
benchmark)
	go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH031AutomaticJSONSubcolumn$' -benchmem -count=3
	;;
*)
	printf 'unknown mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
