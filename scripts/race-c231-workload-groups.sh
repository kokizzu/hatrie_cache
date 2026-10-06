#!/usr/bin/env bash
set -euo pipefail
cache_dir=$(mktemp -d /tmp/hatrie-c233-cpu-budget-gocache.XXXXXX)
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test -race ./hat/hatSql -run 'TestNamespaceQueryMemory|TestNamespaceResourceMemory|TestSQLQueryMemory' -count=1
