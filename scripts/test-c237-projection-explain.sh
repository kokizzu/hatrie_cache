#!/usr/bin/env bash
set -euo pipefail

cache_dir="$(mktemp -d /tmp/hatrie-c237-test-cache.XXXXXX)"
trap 'rm -rf "$cache_dir"' EXIT

GOCACHE="$cache_dir" go test ./hat/hatSql -run '^(TestC237|TestMZ044|TestProjectionCatalog|TestSQLProjectionAdvisor)' -count=1
