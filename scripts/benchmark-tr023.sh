#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure \
  -run '^$' \
  -bench '^BenchmarkT023TupleMultikeyIndex$' \
  -benchmem \
  -count=5
