#!/usr/bin/env bash
set -euo pipefail

cache="$PWD/.gocache-chu63-race"
trap 'rm -rf "$cache"' EXIT
GOCACHE="$cache" go test -race ./hat/hatSql -run 'TestCHU63' -count=1
