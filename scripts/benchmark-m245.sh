#!/usr/bin/env bash
set -euo pipefail

repo="${PWD}"
baseline=/tmp/hatrie-cache-m245-baseline
cache=/tmp/hatrie-cache-m245-bench-cache
cleanup() {
  git -C "$repo" worktree remove --force "$baseline" >/dev/null 2>&1 || true
  rm -rf "$cache" "$baseline"
}
trap cleanup EXIT
cleanup
mkdir -p "$cache"
git -C "$repo" worktree add --detach "$baseline" 3a9c3f55999a070368a6d76d93bd016d874c8012 >/dev/null
cp "$repo/hat/hatSql/m245_progress_metrics_baseline_benchmark_test.go" "$baseline/hat/hatSql/m245_progress_metrics_baseline_benchmark_test.go"
printf '%s\n' '--- before: six generic metric updates ---'
cd "$baseline"
GOCACHE="$cache" go test ./hat/hatSql -run '^$' -bench '^BenchmarkM245SixGenericMetricUpdates$' -benchmem -count=5
printf '%s\n' '--- after: one typed progress update ---'
cd "$repo"
GOCACHE="$cache" go test ./hat/hatSql -run '^$' -bench '^BenchmarkM245ProgressUpdate$' -benchmem -count=5
