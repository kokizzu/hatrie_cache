#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH008' -benchmem -benchtime=100x -count=5 -v | tail -n 80
