#!/usr/bin/env bash
set -euo pipefail

tmp_dir="$(mktemp -d /tmp/hatrie-c213-topk-race.XXXXXX)"
trap 'rm -rf "$tmp_dir"' EXIT
mkdir -p "$tmp_dir/cache" "$tmp_dir/tmp"
GORACE="halt_on_error=1" GOCACHE="$tmp_dir/cache" GOTMPDIR="$tmp_dir/tmp" go test -race ./hat/hatSql -run '^TestC213IncrementalTopK' -count=1
