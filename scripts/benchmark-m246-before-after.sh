#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
baseline="/tmp/hatrie-cache-m246-baseline"
cache_dir="$(mktemp -d /tmp/hatrie-cache-m246-gocache.XXXXXX)"
benchmark_file="hat/hatPipeline/m246_history_retention_legacy_benchmark_test.go"

cleanup() {
	git -C "$repo_root" worktree remove --force "$baseline" >/dev/null 2>&1 || true
	rm -rf "$baseline" "$cache_dir"
}
trap cleanup EXIT

if [[ -e "$baseline" ]]; then
	echo "temporary baseline already exists: $baseline" >&2
	exit 1
fi

git -C "$repo_root" worktree add --detach "$baseline" origin/master >/dev/null
cp "$repo_root/$benchmark_file" "$baseline/$benchmark_file"

echo "=== baseline (origin/master; legacy path) ==="
(cd "$baseline" && GOCACHE="$cache_dir" go test -run '^$' -bench '^BenchmarkM246FrontierRetentionAcquireRelease$' -benchmem -benchtime=2s -count=5 ./hat/hatPipeline)

echo "=== candidate (M246; legacy and policy paths) ==="
(cd "$repo_root" && GOCACHE="$cache_dir" go test -run '^$' -bench '^BenchmarkM246FrontierRetention' -benchmem -benchtime=2s -count=5 ./hat/hatPipeline)
