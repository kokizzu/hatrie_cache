#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/roaring.go \
  hat/hatDataStructure/roaring_lookup_fastpath_test.go \
  hat/hatDataStructure/roaring_lookup_fastpath_benchmark_test.go
