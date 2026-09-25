#!/usr/bin/env bash
set -euo pipefail

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-c154f-race.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT

GOCACHE="$tmp_dir/go-cache" go test -race ./hat/hatPipeline -run 'C154fSchemaMigrationBarrierSnapshot' -count=1
