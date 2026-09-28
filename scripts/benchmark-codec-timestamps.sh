#!/usr/bin/env bash
set -euo pipefail

output_file="$(mktemp "${TMPDIR:-/tmp}/hatrie-timestamp-benchmark.XXXXXX")"
trap 'rm -f "$output_file"' EXIT

go test ./hat/hatCodec -run '^$' -bench 'BenchmarkCompactTimestamp' -benchmem -count=5 >"$output_file" 2>&1
awk '
$1 ~ /^BenchmarkCompactTimestamp/ {
    if ($3 !~ /^[0-9]/ || $5 !~ /^[0-9]/ || $7 !~ /^[0-9]/) next
    runs[$1]++
    ns[$1] += $3
    bytes[$1] += $5
    allocs[$1] += $7
}
END {
    for (name in runs) {
        printf "%s runs=%d mean_ns=%.2f mean_bytes=%.2f mean_allocs=%.2f\n", name, runs[name], ns[name] / runs[name], bytes[name] / runs[name], allocs[name] / runs[name]
    }
}' "$output_file"
