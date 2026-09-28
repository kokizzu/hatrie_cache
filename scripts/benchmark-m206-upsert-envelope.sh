#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
worktree="${TMPDIR:-/tmp}/hatrie-m206-bench.$PPID"

cleanup() {
  git -C "$repo_root" worktree remove --force "$worktree" >/dev/null 2>&1 || true
  rm -rf "$worktree"
}
trap cleanup EXIT

git -C "$repo_root" fetch origin master >/dev/null
git -C "$repo_root" worktree add --detach "$worktree" origin/master >/dev/null
cp "$repo_root/hat/hatSql/m206_upsert_envelope.go" "$worktree/hat/hatSql/m206_upsert_envelope.go"
cp "$repo_root/hat/hatSql/m206_upsert_envelope_benchmark_test.go" "$worktree/hat/hatSql/m206_upsert_envelope_benchmark_test.go"
(cd "$worktree" && go test ./hat/hatSql -run '^$' -bench '^BenchmarkM206' -benchmem -count=5)
