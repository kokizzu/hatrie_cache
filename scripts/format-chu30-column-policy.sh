#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatStorage/remote_part.go \
  hat/hatStorage/remote_part_cache.go \
  hat/hatStorage/remote_part_cache_test.go \
  hat/hatStorage/remote_part_cache_baseline_test.go \
  hat/hatSql/z_chu30_compile_shim.go
