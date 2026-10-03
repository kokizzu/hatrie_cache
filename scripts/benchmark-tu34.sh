#!/usr/bin/env bash
set -euo pipefail

output=$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-tu34-benchmark.XXXXXX")
trap 'rm -f "$output"' EXIT
go test ./hat/hatJournal -run '^$' -bench '^Benchmark(SpaceSyncPolicyResolve|SpaceSyncAppenderBoundary)$' -benchmem -benchtime=100ms -count=5 >"${output}"
awk '/^Benchmark(SpaceSyncPolicyResolve|SpaceSyncAppenderBoundary)/ { print }' "${output}"
