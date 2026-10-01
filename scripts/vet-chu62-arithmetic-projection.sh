#!/usr/bin/env bash
set -euo pipefail

cache="$PWD/.gocache-chu62-vet"
trap 'rm -rf "$cache"' EXIT
GOCACHE="$cache" go vet ./hat/hatSql
