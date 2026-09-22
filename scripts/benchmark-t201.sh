#!/usr/bin/env bash
set -euo pipefail

GOMAXPROCS=1 go test ./hat/hatReplication -run '^$' -bench '^BenchmarkT201' -benchmem -count=5
