#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
cache_dir="/tmp/hatrie-cache-ch002-race-gocache"
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test -race ./hat/hatSql -run '^TestCH002' -count=1
