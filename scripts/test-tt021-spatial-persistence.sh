#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-tt021-spatial-persistence.XXXXXX")
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./hat/hatSchema -run '^TestTT021SpatialIndexBinaryPersistenceRoundTripAndValidation$' -count=1 -v
