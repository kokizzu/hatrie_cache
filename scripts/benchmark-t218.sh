#!/usr/bin/env bash
set -euo pipefail

go test -tags=t218 ./hat/hatDataStructure \
  -run '^$' \
  -bench '^BenchmarkT218' \
  -benchmem \
  -count=3 \
  -benchtime=200ms
