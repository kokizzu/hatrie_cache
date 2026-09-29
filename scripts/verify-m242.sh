#!/usr/bin/env bash
set -euo pipefail

repo="${PWD}"
cache=/tmp/hatrie-cache-m242-verify-cache
cleanup() {
  rm -rf "$cache"
}
trap cleanup EXIT
rm -rf "$cache"
mkdir -p "$cache"
printf '%s\n' '--- package tests ---'
GOCACHE="$cache" go test ./hat/hatSql
printf '%s\n' '--- race tests ---'
GOCACHE="$cache" go test -race ./hat/hatSql
printf '%s\n' '--- vet ---'
GOCACHE="$cache" go vet ./hat/hatSql
