#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-go-bench-m052aa.XXXXXX")
trap 'rm -rf -- "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./hat/hatSql -run '^$' -bench '^BenchmarkCompiledSQLAutomaticNativeJoin(Fallback)?$' -benchmem -count=5
