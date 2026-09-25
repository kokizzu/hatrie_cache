#!/usr/bin/env bash
set -euo pipefail

cache_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-t047-reconcile-baseline.XXXXXX")"
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./hat/hatReplication -run '^$' -bench '^BenchmarkTU047ParticipantStatusLoop$' -benchmem -benchtime=200ms -count=5
