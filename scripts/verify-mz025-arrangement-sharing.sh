#!/usr/bin/env bash
set -euo pipefail

cache_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-mz025-verify.XXXXXX")"
trap 'rm -rf "$cache_dir"' EXIT

GOCACHE="$cache_dir" go test ./hat/hatSql -count=1
GOCACHE="$cache_dir" go test ./hat/hatSql -race -run '^TestMZ025AggregateArrangementCanonicalSharing$' -count=1
GOCACHE="$cache_dir" go vet ./hat/hatSql
