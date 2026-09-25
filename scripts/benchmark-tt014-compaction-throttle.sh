#!/usr/bin/env bash
set -euo pipefail

raw=$(mktemp /tmp/hatrie-tt014-controller.XXXXXX)
trap 'rm -f "$raw"' EXIT
if ! go test ./hat/hatStorage -run '^$' -bench 'BenchmarkCompactionControllerPendingBytesTT014' -benchtime=100ms -benchmem -count=5 > "$raw"; then
  cat "$raw"
  exit 1
fi
rg '^Benchmark' "$raw"
