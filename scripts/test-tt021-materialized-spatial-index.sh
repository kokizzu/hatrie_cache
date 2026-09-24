#!/usr/bin/env bash
set -euo pipefail

tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-tt021-spatial-test.XXXXXX")"
trap 'rm -rf "$tmp_dir"' EXIT
GOCACHE="$tmp_dir/go-build" go test ./hat/hatSchema -run '^TestTT021MaterializedSourceSpatialIndex' -count=1
