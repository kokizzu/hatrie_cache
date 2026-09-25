#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-tt024-union-bench.XXXXXX")
trap 'rm -rf "$cache_dir"' EXIT

GOCACHE="$cache_dir" go test ./hat/hatCache -run '^$' -bench '^BenchmarkTT024TextPhraseUnion$' -benchmem -benchtime=100x -count=5
