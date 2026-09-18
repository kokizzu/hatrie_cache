#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline \
  -run '^$' \
  -bench 'BenchmarkMZ038' \
  -benchmem \
  -count=5 \
  -benchtime=500ms
