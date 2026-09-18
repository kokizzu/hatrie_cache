#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'MZ-009 timestamp-domain lease benchmark'
go test ./hat/hatPipeline -run '^$' -bench 'BenchmarkMZ009' -benchmem -count=5 -benchtime=100ms -v
