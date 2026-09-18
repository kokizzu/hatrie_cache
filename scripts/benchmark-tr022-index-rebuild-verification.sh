#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'TR-022 index rebuild verification benchmark'
go test ./hat/hatSql -run '^$' -bench 'BenchmarkTR022' -benchmem -count=5 -benchtime=100ms -v
