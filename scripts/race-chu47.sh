#!/usr/bin/env bash
set -euo pipefail

cache="$PWD/.gocache-chu47-race"
trap 'rm -rf "$cache"' EXIT
GOCACHE="$cache" go test -race ./hat/hatSql -run TestSQLDictionary -count=1
