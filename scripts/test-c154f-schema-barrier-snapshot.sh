#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-c154f-test-cache.XXXXXX")
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./hat/hatPipeline -run '^TestC154fSchemaMigrationBarrierSnapshot' -count=1
