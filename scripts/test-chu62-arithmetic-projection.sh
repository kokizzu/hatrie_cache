#!/usr/bin/env bash
set -euo pipefail

cache="$PWD/.gocache-chu62"
trap 'rm -rf "$cache"' EXIT
GOCACHE="$cache" go test ./hat/hatSql -run 'TestCHU62' -count=1
