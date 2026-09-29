#!/usr/bin/env bash
set -euo pipefail

temp_root=$(mktemp -d /tmp/hatrie-cache-t201-benchmark.XXXXXX)
trap 'rm -rf "$temp_root"' EXIT
mkdir -p "$temp_root/gocache" "$temp_root/gotmp"
BENCHTIME=${BENCHTIME:-200ms}
COUNT=${COUNT:-5}
GOCACHE="$temp_root/gocache" GOTMPDIR="$temp_root/gotmp" go test ./hat/hatReplication -run '^$' -bench '^BenchmarkT201(SpaceWriteQuorumEvaluate|SpaceWriteQuorumCachedPolicy|JournalWriteQuorumEvaluateControl|SpaceWriteQuorumDisabled)$' -benchmem -cpu=1 -benchtime="$BENCHTIME" -count="$COUNT"
