#!/usr/bin/env bash
set -euo pipefail

cache_dir="$(mktemp -d /tmp/hatrie-cache-ch042-benchmark-cache.XXXXXX)"
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH042ColumnarDictionaryGroup$' -benchmem -count=5
