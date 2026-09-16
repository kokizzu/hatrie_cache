#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure \
  -run '^$' \
  -bench 'BenchmarkTT045Tuple' \
  -benchmem \
  -count=5
