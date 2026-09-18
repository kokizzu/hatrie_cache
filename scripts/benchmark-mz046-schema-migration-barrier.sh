#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'MZ-046 schema migration barrier benchmark'
go test ./hat/hatPipeline -run '^$' -bench 'BenchmarkMZ046' -benchmem -count=5 -benchtime=100ms -v
