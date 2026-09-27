#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cache_dir="$(mktemp -d /tmp/hatrie-mz004-priority-race-cache.XXXXXX)"
tmp_dir="$(mktemp -d /tmp/hatrie-mz004-priority-race-tmp.XXXXXX)"
trap 'rm -rf -- "$cache_dir" "$tmp_dir"' EXIT
cd "$repo_root"
GOCACHE="$cache_dir" GOTMPDIR="$tmp_dir" go test -race ./hat/hatPipeline -run '^TestMZ004Priority' -count=1 -timeout=120s
