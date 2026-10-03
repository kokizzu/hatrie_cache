#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure \
  -run '^$' \
  -bench '^BenchmarkT039SpaceChangefeed$' \
  -benchmem \
  -count=5
