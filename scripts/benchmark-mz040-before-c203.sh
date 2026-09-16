#!/usr/bin/env bash
set -euo pipefail

repo_root=$(pwd)
tmp_root=$(mktemp -d /tmp/hatrie-cache-mz040-before.XXXXXX)
cleanup() {
  git -C "$repo_root" worktree remove --force "$tmp_root" >/dev/null 2>&1 || true
  rm -rf "$tmp_root"
}
trap cleanup EXIT

git -C "$repo_root" worktree add --detach "$tmp_root" HEAD >/dev/null
(cd "$tmp_root" && go test ./hat/hatSql -run '^$' -bench '^BenchmarkRecursiveReachabilityMaintenance$' -benchmem -benchtime=20ms -count=3)
