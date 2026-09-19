#!/usr/bin/env bash
set -euo pipefail

output=/tmp/hatrie-cache-tu34-benchmark.txt
go test ./hat/hatJournal -run '^$' -bench '^BenchmarkSpaceSyncPolicyResolve$' -benchmem -count=5 >"${output}"
printf '%s\n' "raw benchmark: ${output}"
awk '/^BenchmarkSpaceSyncPolicyResolve/ { print }' "${output}"
