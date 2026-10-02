#!/usr/bin/env bash
set -euo pipefail

cache="${GOCACHE:-$PWD/.gocache}"
mkdir -p "$cache"
GOCACHE="$cache" go test -race ./hat/hatReplication -run '^TestConflictEvent' -count=1
