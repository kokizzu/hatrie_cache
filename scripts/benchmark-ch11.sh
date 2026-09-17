#!/bin/sh
set -eu

baseline_dir="${TMPDIR:-/tmp}/hatrie-cache-ch11-baseline-$$"
baseline_commit=${CH11_BASELINE_COMMIT:-$(git rev-parse HEAD)}

cleanup() {
	git worktree remove --force "$baseline_dir" >/dev/null 2>&1 || true
}
trap cleanup EXIT HUP INT TERM

git worktree add --detach "$baseline_dir" "$baseline_commit" >/dev/null
printf '%s\n' "--- baseline ${baseline_commit} ---"
(cd "$baseline_dir" && go test ./hat/hatCache -run '^$' -bench='^BenchmarkExecuteCacheCommandWriteQuorum(Disabled|Loopback)$' -benchmem -count=5 -benchtime=100ms)

printf '%s\n' '--- current legacy/default path ---'
go test ./hat/hatCache -run '^$' -bench='^BenchmarkExecuteCacheCommandWriteQuorum(Disabled|Loopback)$' -benchmem -count=5 -benchtime=100ms

printf '%s\n' '--- current request-level quorum path ---'
go test ./hat/hatCache -run '^$' -bench='^BenchmarkExecuteCacheCommandInsertQuorum$' -benchmem -count=5 -benchtime=100ms
