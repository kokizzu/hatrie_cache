#!/usr/bin/env bash
set -euo pipefail

GOMAXPROCS=1 go test -run '^$' -bench '^BenchmarkT234SQLTransaction' -benchmem -benchtime=300ms -count=7 ./hat/hatCache
