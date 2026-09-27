#!/usr/bin/env bash
set -euo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root_dir"
gofmt -w \
  hat/hatCache/mz011_file_sink.go \
  hat/hatCache/mz011_file_sink_test.go \
  hat/hatCache/mz011_file_sink_benchmark_test.go
