#!/usr/bin/env bash
set -euo pipefail

temp_root=$(mktemp -d /tmp/hatrie-cache-t203-benchmark.XXXXXX)
trap 'rm -rf "$temp_root"' EXIT
mkdir -p "$temp_root/gocache" "$temp_root/gotmp"
GOCACHE="$temp_root/gocache" GOTMPDIR="$temp_root/gotmp" go test ./hat/hatTopology -run '^$' -bench '^BenchmarkT203(LeaderLeaseWithFence|LeaderLeaseValidateControl|ValidateThenWriteControl)$' -benchmem -cpu=1 -benchtime=200ms -count=5
