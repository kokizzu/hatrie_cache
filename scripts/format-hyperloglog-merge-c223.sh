#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/hyperloglog.go \
  hat/hatDataStructure/hyperloglog_merge_test.go \
  hat/hatDataStructure/hyperloglog_merge_benchmark_test.go
