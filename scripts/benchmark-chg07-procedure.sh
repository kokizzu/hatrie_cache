#!/usr/bin/env bash
set -euo pipefail

cache=$(mktemp -d /tmp/hatrie-cache-chg07-gocache.XXXXXX)
trap 'rm -rf "$cache"' EXIT
GOCACHE="$cache" go test ./hat/hatSql -run '^$' -bench '^BenchmarkTU03SQLProcedureRegistry$' -benchmem -benchtime=100ms -count=5
