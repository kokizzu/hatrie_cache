#!/usr/bin/env bash
set -euo pipefail

worktree=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-mz027-baseline.XXXXXX")
cleanup() {
  git worktree remove --force "$worktree" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git worktree add --detach "$worktree" HEAD >/dev/null
(cd "$worktree" && go test ./hat/hatSql -run '^$' -bench '^BenchmarkTypedTableAggregateArrangements$' -benchmem -benchtime=1s -count=5)
