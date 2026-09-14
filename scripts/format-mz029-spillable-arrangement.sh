#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/spillable_arrangement.go \
  hat/hatDataStructure/spillable_arrangement_test.go \
  hat/hatDataStructure/spillable_arrangement_benchmark_test.go
