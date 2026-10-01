#!/usr/bin/env bash
set -euo pipefail

cache="$PWD/.gocache-chu63-vet"
trap 'rm -rf "$cache"' EXIT
GOCACHE="$cache" go vet ./hat/hatSql
