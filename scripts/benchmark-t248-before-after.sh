#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
baseline_dir="$(mktemp -d /tmp/hatrie-cache-t248-baseline.XXXXXX)"
cache_dir="$(mktemp -d /tmp/hatrie-cache-t248-bench.XXXXXX)"
cleanup() {
  git -C "$repo_root" worktree remove --force "$baseline_dir" >/dev/null 2>&1 || true
  rm -rf "$cache_dir"
}
trap cleanup EXIT

git -C "$repo_root" worktree add --detach "$baseline_dir" origin/master
cp "$repo_root/hat/hatDataStructure/t248_dead_letter_legacy_benchmark_test.go" "$baseline_dir/hat/hatDataStructure/"

echo "origin/master"
(cd "$baseline_dir" && GOMAXPROCS=1 GOCACHE="$cache_dir" go test -run '^$' -bench '^BenchmarkT248VisibilityQueueNack$' -benchmem -benchtime=2s -count=7 ./hat/hatDataStructure)
echo "candidate"
(cd "$repo_root" && GOMAXPROCS=1 GOCACHE="$cache_dir" go test -run '^$' -bench '^BenchmarkT248VisibilityQueue' -benchmem -benchtime=2s -count=7 ./hat/hatDataStructure)
