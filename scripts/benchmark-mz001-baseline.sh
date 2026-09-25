#!/usr/bin/env bash
set -euo pipefail

cache_dir=$(mktemp -d /tmp/hatrie-cache-mz001-baseline-XXXXXX)
output=$(mktemp /tmp/hatrie-cache-mz001-baseline-output-XXXXXX)
trap 'rm -rf "$cache_dir" "$output"' EXIT
set +e
GOCACHE="$cache_dir" go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkMZ001SourceReplayBaseline$' -benchmem -count=5 -benchtime=100ms -v >"$output" 2>&1
status=$?
set -e
sed -n '1,160p' "$output"
exit "$status"
