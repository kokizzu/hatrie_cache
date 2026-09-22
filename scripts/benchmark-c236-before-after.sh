#!/usr/bin/env bash
set -euo pipefail

baseline_dir=$(mktemp -d /tmp/hatrie-c236-baseline.XXXXXX)
cleanup() {
	git worktree remove --force "$baseline_dir" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git worktree add --detach "$baseline_dir" HEAD >/dev/null
printf '%s\n' '=== C230 baseline ==='
(cd "$baseline_dir" && GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH024ExplainSegmentSkip$' -benchmem -benchtime=2s -count=5)
printf '%s\n' '=== C236 current ==='
GOMAXPROCS=1 go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH024ExplainSegmentSkip$' -benchmem -benchtime=2s -count=5
