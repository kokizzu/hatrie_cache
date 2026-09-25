#!/usr/bin/env bash
set -euo pipefail

cache_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-t047-reconcile.XXXXXX")"
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./hat/hatReplication -run '^$' -bench '^BenchmarkTU047Participant(StatusLoop|BatchReconcile)$' -benchmem -benchtime=200ms -count=5
