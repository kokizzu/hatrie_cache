#!/bin/sh
set -eu

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-c153e-package-cache.XXXXXX")
trap 'rm -rf "$cache_dir"' EXIT HUP INT TERM
GOCACHE="$cache_dir" go test ./hat/hatTopology -count=1
