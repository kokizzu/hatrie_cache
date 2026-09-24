#!/usr/bin/env bash
set -euo pipefail

cache_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-tt021-verify.XXXXXX")"
trap 'rm -rf "$cache_dir"' EXIT

GOCACHE="$cache_dir" go test ./hat/hatSchema -run '^TestTT021SpatialIndexBinaryPersistenceRoundTripAndValidation$' -race -count=1
GOCACHE="$cache_dir" go vet ./hat/hatSchema
