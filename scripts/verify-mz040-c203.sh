#!/usr/bin/env bash
set -euo pipefail

repo_root=$(pwd)
tmp_root=$(mktemp -d /tmp/hatrie-cache-mz040-verify.XXXXXX)
cleanup() {
  git -C "$repo_root" worktree remove --force "$tmp_root" >/dev/null 2>&1 || true
  rm -rf "$tmp_root"
}
trap cleanup EXIT

git -C "$repo_root" worktree add --detach "$tmp_root" HEAD >/dev/null
mkdir -p "$tmp_root/hat/hatSql"
cp "$repo_root/hat/hatSql/mz040_recursive_diagnostics.go" "$tmp_root/hat/hatSql/mz040_recursive_diagnostics.go"
cp "$repo_root/hat/hatSql/mz040_recursive_diagnostics_test.go" "$tmp_root/hat/hatSql/mz040_recursive_diagnostics_test.go"
cp "$repo_root/hat/hatSql/mz040_recursive_diagnostics_benchmark_test.go" "$tmp_root/hat/hatSql/mz040_recursive_diagnostics_benchmark_test.go"
(cd "$tmp_root" && go test ./hat/hatSql -count=1)
