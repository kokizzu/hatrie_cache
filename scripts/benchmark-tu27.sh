#!/usr/bin/env bash
set -euo pipefail

output=/tmp/hatrie-cache-tu27-benchmark.txt
go test ./hat/hatTopology -run '^$' -bench '^BenchmarkConfigWatch(PublishRead|PrefixRead)$' -benchmem -count=5 >"${output}"
printf '%s\n' "raw benchmark: ${output}"
awk '/^BenchmarkConfigWatch(PublishRead|PrefixRead)/ { print }' "${output}"
