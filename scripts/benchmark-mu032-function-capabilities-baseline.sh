#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql \
  -run '^$' \
  -bench 'BenchmarkMU032FunctionMetadata/before_execution$' \
  -benchmem \
  -benchtime=1s \
  -count=5 \
  -cpu=1
