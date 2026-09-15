#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure \
  -run '^$' \
  -bench '^BenchmarkHyperLogLogMergeC223$' \
  -benchmem \
  -benchtime=200ms \
  -count=3
