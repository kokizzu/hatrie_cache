#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'MZ-035 arrangement locality benchmark'
go test ./hat/hatSql -run '^$' -bench 'BenchmarkMZ035' -benchmem -count=5 -benchtime=100ms -v
