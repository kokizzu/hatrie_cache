#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication \
  -run '^$' \
  -bench '^BenchmarkChangefeedCheckpointOperations' \
  -benchmem \
  -count=5
