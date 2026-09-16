#!/usr/bin/env bash
set -euo pipefail

repo=$(pwd)
worktree=$(mktemp -d /tmp/hatrie-cache-ch018-benchmark.XXXXXX)
cleanup() {
	git -C "$repo" worktree remove --force "$worktree" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git -C "$repo" worktree add --detach "$worktree" HEAD >/dev/null
cd "$worktree"

printf '%s\n' 'CH-18 baseline: existing coalesced projection refresh before status instrumentation'
go test ./hat/hatSql -run '^$' -bench '^BenchmarkIncrementalProjectionCoalescedRefresh/coalesced_journal_batch$' -benchmem -benchtime=200ms -count=5

cp "$repo/hat/hatSql/ch018_projection_refresh_status.go" "$worktree/hat/hatSql/"
cp "$repo/hat/hatSql/ch018_projection_refresh_status_benchmark_test.go" "$worktree/hat/hatSql/"
cp "$repo/hat/hatSql/incremental_projection.go" "$worktree/hat/hatSql/"

printf '%s\n' 'CH-18 instrumented: coalesced refresh plus status/control snapshots'
go test ./hat/hatSql -run '^$' -bench 'BenchmarkIncrementalProjectionCoalescedRefresh|BenchmarkCH018Projection' -benchmem -benchtime=200ms -count=5
