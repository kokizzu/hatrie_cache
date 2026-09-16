#!/usr/bin/env bash
set -euo pipefail

repo_root=$(pwd)
tmp_root=$(mktemp -d /tmp/hatrie-cache-mz040-red.XXXXXX)
cleanup() {
  git -C "$repo_root" worktree remove --force "$tmp_root" >/dev/null 2>&1 || true
  rm -rf "$tmp_root"
}
trap cleanup EXIT

git -C "$repo_root" worktree add --detach "$tmp_root" HEAD >/dev/null
mkdir -p "$tmp_root/hat/hatSql"
cp "$repo_root/hat/hatSql/mz040_recursive_diagnostics_test.go" "$tmp_root/hat/hatSql/mz040_recursive_diagnostics_test.go"

if (cd "$tmp_root" && go test ./hat/hatSql -run 'TestMutableRecursiveReachabilityApplyWithOptions' -count=1); then
  printf '%s\n' 'MZ-40 red test unexpectedly passed on the clean baseline'
  exit 1
fi
printf '%s\n' 'MZ-40 red test failed on the clean baseline as expected'
