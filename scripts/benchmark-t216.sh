#!/usr/bin/env bash
set -euo pipefail

go test -tags=t216 ./hat/hatDataStructure \
  -run '^$' \
  -bench '^BenchmarkT216StorageSpace' \
  -benchtime=3s -count=3 -benchmem
