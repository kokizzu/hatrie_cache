#!/usr/bin/env bash
set -euo pipefail

cache="$PWD/.gocache-chu62-all"
trap 'rm -rf "$cache"' EXIT
GOCACHE="$cache" go test ./... -count=1
