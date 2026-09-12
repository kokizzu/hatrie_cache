#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"
output=$(mktemp)
trap 'rm -f "$output"' EXIT
go test ./hat/hatReplication -run '^$' -bench '^BenchmarkBuildReplicaRPOStatuses64$' -benchmem -count=5 >"$output"
rg '^BenchmarkBuildReplicaRPOStatuses64' "$output"
