#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-c153c-benchmark-cache.XXXXXX")
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkC153cOwnership(JSONSnapshotBaseline|BinarySnapshot|JSONRestoreBaseline|BinaryRestore)$' -benchmem -benchtime=200ms -count=5
