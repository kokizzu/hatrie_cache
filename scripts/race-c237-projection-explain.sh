#!/usr/bin/env bash
set -euo pipefail

cache_dir="$(mktemp -d /tmp/hatrie-c237-race-cache.XXXXXX)"
trap 'rm -rf "$cache_dir"' EXIT

GOCACHE="$cache_dir" go test -race ./hat/hatSql -run '^(TestMZ044|TestProjectionCatalog|TestSQLProjectionAdvisor)' -count=1
