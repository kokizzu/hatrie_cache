#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatTopology \
  -run '^$' \
  -bench '^BenchmarkT202LeaderForKey$' \
  -benchmem \
  -count=5
