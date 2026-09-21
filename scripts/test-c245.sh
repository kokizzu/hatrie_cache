#!/usr/bin/env bash
set -euo pipefail

go test \
  hat/hatStorage/remote_part.go \
  hat/hatStorage/remote_part_cache.go \
  hat/hatStorage/remote_part_cache_c245_test.go \
  -run '^TestC245' -count=1
