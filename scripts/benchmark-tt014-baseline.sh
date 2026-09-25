#!/usr/bin/env bash
set -euo pipefail

raw=$(mktemp /tmp/hatrie-tt014-baseline.XXXXXX)
trap 'rm -f "$raw"' EXIT
if ! go test ./hat/hatStorage -run '^$' -bench 'BenchmarkCompactionScheduler(RunC207|Stats|Run)$' -benchtime=100ms -benchmem -count=5 > "$raw"; then
  cat "$raw"
  exit 1
fi
rg '^(goos|goarch|pkg:|cpu:|Benchmark)' "$raw"
