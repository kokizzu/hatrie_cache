#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'TR-037 deadlock detection benchmark'
go test ./hat/hatSql -run '^$' -bench 'BenchmarkTR037' -benchmem -count=5 -benchtime=100ms -v
