#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication \
  -run '^$' \
  -bench '^BenchmarkChangefeedFrontier' \
  -benchmem \
  -count=5
