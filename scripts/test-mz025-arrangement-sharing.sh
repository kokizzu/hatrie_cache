#!/usr/bin/env bash
set -euo pipefail

cache_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-mz025-test.XXXXXX")"
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./hat/hatSql -run '^TestMZ025AggregateArrangementCanonicalSharing$' -count=1 -v
