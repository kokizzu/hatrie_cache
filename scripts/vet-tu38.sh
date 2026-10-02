#!/usr/bin/env bash
set -euo pipefail

cache="${GOCACHE:-$PWD/.gocache}"
mkdir -p "$cache"
GOCACHE="$cache" go vet ./hat/hatReplication
