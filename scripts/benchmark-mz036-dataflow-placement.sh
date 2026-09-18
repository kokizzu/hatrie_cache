#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline \
  -run '^$' \
  -bench 'BenchmarkMZ036' \
  -benchmem \
  -count=5 \
  -benchtime=500ms
