#!/usr/bin/env bash
set -euo pipefail

cache="${GOCACHE:-$PWD/.gocache}"
mkdir -p "$cache"
GOCACHE="$cache" go test ./hat/hatReplication -run '^$' -bench '^BenchmarkT038ConflictResolution/(baseline_resolve|opt_in_recorded_resolve)$|^BenchmarkT038ConflictEventSnapshot$' -benchmem -benchtime=500ms -count=1
