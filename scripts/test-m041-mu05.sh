#!/usr/bin/env bash
set -euo pipefail

cache_dir="/tmp/hatrie-cache-m041-test-cache-$$"
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./hat/hatSql -run 'TestTypedTableArrangementRecovery' -count=1
