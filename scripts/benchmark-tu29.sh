#!/usr/bin/env bash
set -euo pipefail

output=/tmp/hatrie-cache-tu29-benchmark.txt
go test ./hat/hatPeer -run '^$' -bench '^BenchmarkStreamTransactionRecovery' -benchtime=200ms -benchmem -count=5 >"${output}"
printf '%s\n' "raw benchmark: ${output}"
awk '/^BenchmarkStreamTransactionRecovery/ { print }' "${output}"
