#!/usr/bin/env bash
set -euo pipefail

repo="${PWD}"
baseline=/tmp/hatrie-cache-m242-baseline
cache=/tmp/hatrie-cache-m242-bench-cache
cleanup() {
  git -C "$repo" worktree remove --force "$baseline" >/dev/null 2>&1 || true
  rm -rf "$cache" "$baseline"
}
trap cleanup EXIT
cleanup
mkdir -p "$cache"
git -C "$repo" worktree add --detach "$baseline" bd76da5d >/dev/null
cp "$repo/hat/hatSql/m242_operator_metrics_baseline_benchmark_test.go" "$baseline/hat/hatSql/m242_operator_metrics_baseline_benchmark_test.go"
printf '%s\n' '--- before: three generic operator metric updates ---'
cd "$baseline"
GOCACHE="$cache" go test ./hat/hatSql -run '^$' -bench '^BenchmarkM242ThreeGenericOperatorUpdates$' -benchmem -count=5
printf '%s\n' '--- after: one typed operator metric update ---'
cd "$repo"
GOCACHE="$cache" go test ./hat/hatSql -run '^$' -bench '^BenchmarkM242TypedOperatorUpdate$' -benchmem -count=5
