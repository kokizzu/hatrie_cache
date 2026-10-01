#!/usr/bin/env bash
set -euo pipefail

cache="$PWD/.gocache-chu63-package"
trap 'rm -rf "$cache"' EXIT
GOCACHE="$cache" go test ./hat/hatSql -count=1
