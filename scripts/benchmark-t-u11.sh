#!/usr/bin/env bash
set -euo pipefail

output=$(mktemp)
trap 'rm -f "$output"' EXIT
go test ./hat/hatReplication -run '^$' -bench '^BenchmarkConflictPolicyResolution$' -benchmem -count=5 >"$output"
rg '^BenchmarkConflictPolicyResolution' "$output"
