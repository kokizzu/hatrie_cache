#!/usr/bin/env bash
set -euo pipefail

repo_root=$(pwd)
baseline_dir="/tmp/hatrie-cache-m238-baseline-$$"
if [[ -e "$baseline_dir" ]]; then
  echo "refusing to reuse existing baseline path: $baseline_dir" >&2
  exit 1
fi

cleanup() {
  if [[ -d "$baseline_dir" ]]; then
    git worktree remove --force "$baseline_dir"
  fi
}
trap cleanup EXIT

git worktree add --detach "$baseline_dir" HEAD
cp "$repo_root/hat/hatSql/m238_explain_pushdown_test.go" "$baseline_dir/hat/hatSql/m238_explain_pushdown_test.go"

echo "===== M238 parent baseline ====="
pushd "$baseline_dir" >/dev/null
go test ./hat/hatSql -run '^$' -bench '^BenchmarkM238ExplainQuery$' -benchmem -count=5 -benchtime=500ms
popd >/dev/null

echo "===== M238 current tree ====="
go test ./hat/hatSql -run '^$' -bench '^BenchmarkM238ExplainQuery$' -benchmem -count=5 -benchtime=500ms
