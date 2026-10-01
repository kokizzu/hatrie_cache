#!/usr/bin/env bash
set -euo pipefail

cache="$PWD/.gocache-chu62-race"
trap 'rm -rf "$cache"' EXIT
GOCACHE="$cache" go test -race ./hat/hatSql -run 'TestCHU62' -count=1
