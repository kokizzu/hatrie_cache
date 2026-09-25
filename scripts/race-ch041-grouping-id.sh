#!/usr/bin/env bash
set -euo pipefail

cache_dir="$(mktemp -d /tmp/hatrie-cache-race-ch041-grouping-id.XXXXXX)"
trap 'rm -rf -- "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test -race ./hat/hatSql -run '^TestSQLGroupingID' -count=1
