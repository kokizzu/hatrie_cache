#!/usr/bin/env bash
set -euo pipefail

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-c154f-vet.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT

GOCACHE="$tmp_dir/go-cache" go vet ./hat/hatPipeline
