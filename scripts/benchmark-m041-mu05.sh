#!/usr/bin/env bash
set -euo pipefail

cache_dir="/tmp/hatrie-cache-m041-benchmark-cache-$$"
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./hat/hatSql -run '^$' -bench '^BenchmarkTypedTableArrangementRecoveryBundle$' -benchmem -count=5
