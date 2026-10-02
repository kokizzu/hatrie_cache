#!/usr/bin/env bash
set -euo pipefail

cache="${TMPDIR:-/tmp}/hatrie-cache-round61-ch058-test-gocache"
rm -rf "$cache"
trap 'rm -rf "$cache"' EXIT
GOCACHE="$cache" go test ./hat/hatCache -run 'TestSQLBitmapIndexBatchINResolver' -count=1
