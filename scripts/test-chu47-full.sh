#!/usr/bin/env bash
set -euo pipefail

cache="$PWD/.gocache-chu47-full"
trap 'rm -rf "$cache"' EXIT
GOCACHE="$cache" go test ./...
