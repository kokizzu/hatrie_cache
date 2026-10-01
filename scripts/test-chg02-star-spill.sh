#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_dir="$repo_root/.chg02-star-spill-tmp"
cleanup() {
	rm -rf "$tmp_dir"
}
trap cleanup EXIT
cleanup
mkdir -p "$tmp_dir"
export TMPDIR="$tmp_dir"
export GOCACHE="$tmp_dir/go-cache"
go test ./hat/hatSql -run '^TestCHG02ExternalOrderBySelectStar' -count=1
