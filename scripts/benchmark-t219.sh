#!/usr/bin/env bash
set -euo pipefail

go test -tags=t219 ./hat/hatDataStructure \
  -run '^$' \
  -bench '^BenchmarkT219' \
  -benchmem \
  -count=3 \
  -benchtime=200ms
