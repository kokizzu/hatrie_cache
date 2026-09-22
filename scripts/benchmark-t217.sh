#!/usr/bin/env bash
set -euo pipefail

go test -tags=t217 ./hat/hatDataStructure \
  -run '^$' \
  -bench '^BenchmarkT217' \
  -benchtime=3s -count=3 -benchmem
