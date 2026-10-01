#!/usr/bin/env bash
set -euo pipefail

cache=/tmp/hatrie-cache-chg14-gocache
cleanup() {
  rm -rf "$cache"
}
trap cleanup EXIT

mkdir -p "$cache"
GOCACHE="$cache" ./scripts/verify-go.sh
