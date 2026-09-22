#!/usr/bin/env bash
set -euo pipefail

GOMAXPROCS=1 go test ./hat/hatReplication -run '^$' -bench '^BenchmarkT204' -benchmem -count=5
