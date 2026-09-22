#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/memtx_table.go \
  hat/hatDataStructure/t231_after_replace_test.go
