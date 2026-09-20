#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/vertical_ttl_delete.go \
  hat/hatDataStructure/vertical_ttl_delete_test.go \
  hat/hatDataStructure/vertical_ttl_delete_benchmark_test.go
