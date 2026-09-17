#!/bin/sh
set -eu

baseline=/tmp/hatrie-cache-chu23-baseline-worktree
cleanup() {
	git worktree remove --force "$baseline" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

if [ -e "$baseline" ]; then
	git worktree remove --force "$baseline"
fi
git worktree add --detach "$baseline" origin/master
cp hat/hatCache/chu23_async_insert_submit_benchmark_test.go "$baseline/hat/hatCache/"
(cd "$baseline" && GOMAXPROCS=1 go test ./hat/hatCache -run '^$' -bench '^BenchmarkCHU23AsyncInsertSubmitBaseline$' -benchmem -cpu=1 -count=5)
