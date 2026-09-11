#!/usr/bin/env bash
set -euo pipefail

worktree=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-tt036-baseline.XXXXXX")
cleanup() {
	git worktree remove --force "$worktree" >/dev/null 2>&1 || true
}
trap cleanup EXIT
git worktree add --detach "$worktree" HEAD >/dev/null
(cd "$worktree" && go test ./hat/hatStorage -run '^$' -bench '^BenchmarkCompactionSchedulerRun$' -benchmem -benchtime=1s -count=5)
