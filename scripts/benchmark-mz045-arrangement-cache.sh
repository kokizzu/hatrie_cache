#!/usr/bin/env bash
set -euo pipefail

cache_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-mz045-benchmark.XXXXXX")"
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ045ArrangementMetadata' -benchmem -benchtime=100x -count=5
