#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH009TTL' -benchmem -benchtime=100x -count=5 -v
