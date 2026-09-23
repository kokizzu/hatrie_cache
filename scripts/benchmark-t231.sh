#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure \
  -run '^$' \
  -bench '^BenchmarkT231Space(PutBaseline|AfterReplace)$' \
  -benchmem \
  -count=5 \
  -cpu=1
