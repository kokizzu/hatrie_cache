#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cache_dir="$(mktemp -d /tmp/hatrie-mz004-priority-benchmark-cache.XXXXXX)"
tmp_dir="$(mktemp -d /tmp/hatrie-mz004-priority-benchmark-tmp.XXXXXX)"
trap 'rm -rf -- "$cache_dir" "$tmp_dir"' EXIT
cd "$repo_root"
GOCACHE="$cache_dir" GOTMPDIR="$tmp_dir" go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkMZ004(Priority|FIFO)' -benchmem -benchtime=100ms -count=5
