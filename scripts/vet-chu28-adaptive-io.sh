#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cache=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-chu28-gocache.XXXXXX")
trap 'rm -rf "$cache"' EXIT
(cd "$repo" && GOCACHE="$cache" go vet ./hat/hatStorage)
